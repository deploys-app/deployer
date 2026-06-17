package k8s

import (
	"maps"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"
)

var defaultEphemeralStorage = resource.MustParse("20Mi")

func intstrIntPtr(val int) *intstr.IntOrString {
	p := intstr.FromInt(val)
	return &p
}

func optV1TimeToTime(t *metav1.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.Time
}

func boolPtrToBool(val *bool) bool {
	if val == nil {
		return false
	}
	return *val
}

type Disk struct {
	Name      string
	MountPath string
	SubPath   string
}

func cloneLabels(l map[string]string) map[string]string {
	r := make(map[string]string)
	maps.Copy(r, l)
	return r
}

func nonSpotNodeAffinity() *v1.NodeAffinity {
	return &v1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
			NodeSelectorTerms: []v1.NodeSelectorTerm{{
				MatchExpressions: []v1.NodeSelectorRequirement{
					{
						Key:      "cloud.google.com/gke-spot",
						Operator: v1.NodeSelectorOpDoesNotExist,
					},
				},
			}},
		},
	}
}

func preferNonSpotNodeAffinity() *v1.NodeAffinity {
	return &v1.NodeAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{
			{
				Weight: 1,
				Preference: v1.NodeSelectorTerm{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      "cloud.google.com/gke-spot",
							Operator: v1.NodeSelectorOpDoesNotExist,
						},
					},
				},
			},
		},
	}
}

func defaultSpotNodeAffinity() *v1.NodeAffinity {
	return &v1.NodeAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{
			{
				Weight: 1,
				Preference: v1.NodeSelectorTerm{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      "cloud.google.com/gke-spot",
							Operator: v1.NodeSelectorOpExists,
						},
					},
				},
			},
		},
	}
}

func preferSpotNodeAffinity() *v1.NodeAffinity {
	return &v1.NodeAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{
			{
				Weight: 90,
				Preference: v1.NodeSelectorTerm{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      "cloud.google.com/gke-spot",
							Operator: v1.NodeSelectorOpExists,
						},
					},
				},
			},
		},
	}
}

func imagePullPolicy(image string) v1.PullPolicy {
	if strings.Contains(image, "@sha256:") {
		return v1.PullIfNotPresent
	}
	return v1.PullAlways
}

func securityContext() *v1.PodSecurityContext {
	return &v1.PodSecurityContext{
		RunAsUser:    pointer.Int64(10000),
		RunAsGroup:   pointer.Int64(10000),
		RunAsNonRoot: pointer.Bool(true),
		FSGroup:      pointer.Int64(10000),
	}
}
