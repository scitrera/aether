package gateway

import (
	"google.golang.org/grpc/keepalive"
	"time"
)

// StreamKeepaliveParameters rotates connections without aborting active Connect
// streams. Stream age is not a task deadline. Zero age grace selects gRPC's
// unlimited drain period: new streams move to a new connection after GOAWAY,
// while existing service sessions finish normally.
func StreamKeepaliveParameters() keepalive.ServerParameters {
	return keepalive.ServerParameters{
		MaxConnectionIdle: 15 * time.Minute,
		MaxConnectionAge:  2 * time.Hour,
		Time:              30 * time.Second,
		Timeout:           10 * time.Second,
	}
}
