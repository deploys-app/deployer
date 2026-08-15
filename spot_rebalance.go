package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/deploys-app/deployer/k8s"
)

const (
	spotRebalanceInterval    = 60 * time.Second
	spotRebalanceTickTimeout = 30 * time.Second
)

func runSpotRebalance(client *k8s.Client) {
	for range time.Tick(spotRebalanceInterval) {
		ctx, cancel := context.WithTimeout(context.Background(), spotRebalanceTickTimeout)
		if err := client.RebalanceSpot(ctx); err != nil {
			slog.Error("spot rebalance tick failed", "error", err)
		}
		cancel()
	}
}
