package grpctransport

import (
	"time"

	"google.golang.org/grpc/backoff"
)

func backoffCfg() backoff.Config {
	return backoff.Config{
		BaseDelay:  100 * time.Millisecond,
		Multiplier: 1.6,
		Jitter:     0.2,
		MaxDelay:   2 * time.Second,
	}
}
