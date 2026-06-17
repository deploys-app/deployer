package redis

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/deploys-app/deployer/k8s"
)

var configOverride = os.Getenv("DATABASE_REDIS_CONFIG_OVERRIDE")

func init() {
	if configOverride == "" {
		configOverride = "CONFIG"
	}
}

type Deployer struct {
	Client *k8s.Client
}

type Config struct {
	ID         string
	Name       string
	ProjectID  string
	Image      string
	DBSize     int64 // Mi
	Databases  int64
	Args       []string
	RequestCPU string
	LimitCPU   string
	Password   string
}

func (d *Deployer) Deploy(ctx context.Context, r Config) error {
	// get old revision
	old, err := d.Client.GetReplicaSet(ctx, r.ID)
	if err != nil {
		return err
	}

	var (
		revision   int64
		masterauth string
	)
	if old != nil {
		revision, _ = strconv.ParseInt(old.ObjectMeta.Labels["revision"], 10, 64)
		revision++

		for _, x := range old.Spec.Template.Spec.Containers[0].Args {
			if strings.HasPrefix(x, "--requirepass") {
				masterauth = strings.TrimPrefix(x, "--requirepass ")
			}
		}
	}
	if revision <= 0 {
		revision = 1
	}

	errorCleanupNewRevision := true
	defer func() {
		if !errorCleanupNewRevision {
			return
		}

		slog.Error("redis: deploy error, cleanup new replica set", "id", r.ID)

		// delete new replica set
		d.Client.DeleteReplicaSet(ctx, old.ObjectMeta.Name)
		d.Client.DeletePersistentVolumeClaim(ctx, old.ObjectMeta.Name)
	}()

	// create new revision
	{
		// create disk
		slog.Info("redis: creating disk", "id", r.ID)
		err := d.Client.CreatePersistentVolumeClaimForReplicaSet(ctx, k8s.PersistentVolumeClaimForReplicaSet{
			ID:           r.ID,
			ProjectID:    r.ProjectID,
			Revision:     revision,
			Size:         calcDiskSizeGi(r.DBSize),
			StorageClass: "ssd",
		})
		if err != nil {
			return err
		}

		overhead := max(r.DBSize*5/100, 32)
		memory := fmt.Sprintf("%dMi", r.DBSize+overhead)

		args := []string{
			"--save \"\"",
			"--appendonly yes",
			"--maxmemory " + fmt.Sprintf("%dmb", r.DBSize),
			"--rename-command CONFIG " + configOverride,
		}

		if r.Password != "" {
			args = append(args, "--requirepass "+r.Password)
		}
		if masterauth != "" {
			args = append(args, "--masterauth "+masterauth)
		}
		if r.Databases > 0 {
			args = append(args, "--databases "+strconv.FormatInt(r.Databases, 10))
		}

		allowArgs := []string{
			"--loadmodule ",
			"--maxmemory-policy ",
			"--notify-keyspace-events ",
		}

		hasMaxMemoryPolicy := false
		for _, x := range r.Args {
			for _, a := range allowArgs {
				if strings.HasPrefix(x, a) {
					args = append(args, x)
					break
				}
			}

			if strings.HasPrefix(x, "--maxmemory-policy") {
				hasMaxMemoryPolicy = true
			}
		}
		if !hasMaxMemoryPolicy {
			args = append(args, "--maxmemory-policy allkeys-lru")
		}

		// create replica set
		slog.Info("redis: creating replicaset", "id", r.ID)
		err = d.Client.CreateReplicaSet(ctx, k8s.ReplicaSet{
			ID:            r.ID,
			ProjectID:     r.ProjectID,
			Revision:      revision,
			Replicas:      1,
			Name:          r.Name,
			Image:         r.Image,
			Args:          args,
			ExposePort:    6379,
			RequestCPU:    r.RequestCPU,
			LimitCPU:      r.LimitCPU,
			RequestMemory: memory,
			LimitMemory:   memory,
			Disk: k8s.Disk{
				Name:      fmt.Sprintf("%s-%d", r.ID, revision),
				MountPath: "/data",
			},
		})
		if err != nil {
			return err
		}
	}

	var slaveClient *redis.Client
	if old != nil {
		ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
		defer cancel()

		// migrate data from old revision

		slog.Info("redis: wait new replica set ready", "id", r.ID)
		err = d.Client.WaitReplicaSetReady(ctx, r.ID, revision)
		if err != nil {
			return err
		}

		slog.Info("redis: getting master pod ip", "id", r.ID)
		masterIP, err := d.Client.GetReplicaSetPodIP(ctx, r.ID, revision-1)
		if err != nil {
			return err
		}

		slog.Info("redis: getting slave pod ip", "id", r.ID)
		slaveIP, err := d.Client.GetReplicaSetPodIP(ctx, r.ID, revision)
		if err != nil {
			return err
		}

		masterClient := redis.NewClient(&redis.Options{
			Addr:     masterIP + ":6379",
			Password: masterauth,
		})
		defer masterClient.Close()

		// set new revision to slave
		slaveClient = redis.NewClient(&redis.Options{
			Addr:     slaveIP + ":6379",
			Password: r.Password,
		})
		defer slaveClient.Close()

		slog.Info("redis: config slave", "id", r.ID)
		err = slaveClient.SlaveOf(ctx, masterIP, "6379").Err()
		if err != nil {
			return err
		}

		slog.Info("redis: disable slave-read-only", "id", r.ID)
		err = slaveClient.Do(ctx, configOverride, "set", "slave-read-only", "no").Err()
		if err != nil {
			return err
		}

		// wait slave sync with master
		slog.Info("redis: wait slave sync with master (timeout=10m)", "id", r.ID)
		err = masterClient.Wait(ctx, 1, 10*time.Minute).Err()
		if err != nil {
			slog.Error("redis: wait slave sync with master error", "id", r.ID, "error", err)
			slog.Info("redis: manual wait", "id", r.ID)
			time.Sleep(time.Minute)
		}
	}

	// create/patch service
	slog.Info("redis: creating service", "id", r.ID)
	err = d.Client.CreateServiceForReplicaSet(ctx, k8s.ServiceForReplicaSet{
		ID:         r.ID,
		Revision:   revision,
		ProjectID:  r.ProjectID,
		Port:       6379,
		ExposeNode: false,
	})
	if err != nil {
		return err
	}

	errorCleanupNewRevision = false

	if old != nil {
		// delete old replica set

		slog.Info("redis: deleting old rs", "id", r.ID)
		err = d.Client.DeleteReplicaSet(ctx, old.Name)
		if err != nil {
			return err
		}

		slog.Info("redis: deleting old pvc", "id", r.ID)
		err = d.Client.DeletePersistentVolumeClaim(ctx, old.Name)
		if err != nil {
			return err
		}

		// promote slave to master
		slog.Info("redis: promote slave to master", "id", r.ID)
		err = slaveClient.SlaveOf(ctx, "no", "one").Err()
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Deployer) Delete(ctx context.Context, id string) error {
	rs, err := d.Client.GetReplicaSet(ctx, id)
	if err != nil {
		return err
	}

	// delete service
	slog.Info("redis: deleting service", "id", id)
	err = d.Client.DeleteService(ctx, id)
	if err != nil {
		return err
	}

	// delete replica set
	slog.Info("redis: deleting replicaset", "id", id)
	err = d.Client.DeleteReplicaSet(ctx, rs.Name)
	if err != nil {
		return err
	}

	// delete disk
	err = d.Client.DeletePersistentVolumeClaim(ctx, rs.Name)
	if err != nil {
		return err
	}

	return nil
}

func calcDiskSizeGi(dbSizeMi int64) int64 {
	dbSizeMi *= 2
	if dbSizeMi < 1024 {
		return 1
	}
	r := dbSizeMi / 1024
	if dbSizeMi%1024 > 0 {
		r++
	}
	return r
}
