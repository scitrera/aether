package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/logging"
)

var (
	addr            = flag.String("addr", "localhost:50051", "Gateway address")
	numConnections  = flag.Int("connections", 100, "Number of concurrent connections")
	messagesPerConn = flag.Int("messages", 50, "Messages per connection")
	duration        = flag.Duration("duration", 30*time.Second, "Test duration")
	memProfile      = flag.String("memprofile", "", "Write memory profile to file")
)

type LoadTestStats struct {
	connectionsOpened atomic.Int64
	connectionsClosed atomic.Int64
	messagesSent      atomic.Int64
	messagesReceived  atomic.Int64
	errors            atomic.Int64
	startTime         time.Time
}

func (s *LoadTestStats) Print() {
	elapsed := time.Since(s.startTime)
	fmt.Printf("\n=== Load Test Results ===\n")
	fmt.Printf("Duration: %v\n", elapsed)
	fmt.Printf("Connections opened: %d\n", s.connectionsOpened.Load())
	fmt.Printf("Connections closed: %d\n", s.connectionsClosed.Load())
	fmt.Printf("Messages sent: %d\n", s.messagesSent.Load())
	fmt.Printf("Messages received: %d\n", s.messagesReceived.Load())
	fmt.Printf("Errors: %d\n", s.errors.Load())
	fmt.Printf("Messages/sec: %.2f\n", float64(s.messagesSent.Load())/elapsed.Seconds())

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("\n=== Memory Stats ===\n")
	fmt.Printf("Alloc: %v MB\n", m.Alloc/1024/1024)
	fmt.Printf("TotalAlloc: %v MB\n", m.TotalAlloc/1024/1024)
	fmt.Printf("Sys: %v MB\n", m.Sys/1024/1024)
	fmt.Printf("NumGC: %v\n", m.NumGC)
	fmt.Printf("HeapInuse: %v MB\n", m.HeapInuse/1024/1024)
	fmt.Printf("HeapObjects: %v\n", m.HeapObjects)
	fmt.Printf("StackInuse: %v MB\n", m.StackInuse/1024/1024)
}

func simulateAgent(ctx context.Context, agentID int, stats *LoadTestStats, wg *sync.WaitGroup) {
	defer wg.Done()

	// Connect to gateway
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logging.Logger.Error().Err(err).Int("agent_id", agentID).Msg("failed to connect")
		stats.errors.Add(1)
		return
	}
	defer conn.Close()

	client := pb.NewAetherGatewayClient(conn)
	stream, err := client.Connect(ctx)
	if err != nil {
		logging.Logger.Error().Err(err).Int("agent_id", agentID).Msg("failed to create stream")
		stats.errors.Add(1)
		return
	}

	stats.connectionsOpened.Add(1)

	// Initialize connection
	initMsg := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_Init{
			Init: &pb.InitConnection{
				ClientType: &pb.InitConnection_Agent{
					Agent: &pb.AgentIdentity{
						Workspace:      "loadtest",
						Implementation: fmt.Sprintf("agent-%d", agentID),
						Specifier:      "v1",
					},
				},
			},
		},
	}

	if err := stream.Send(initMsg); err != nil {
		logging.Logger.Error().Err(err).Int("agent_id", agentID).Msg("failed to send init")
		stats.errors.Add(1)
		return
	}

	// Start receiver goroutine
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for {
			_, err := stream.Recv()
			if err != nil {
				return
			}
			stats.messagesReceived.Add(1)
		}
	}()

	// Send messages
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	msgCount := 0
	for {
		select {
		case <-ctx.Done():
			stats.connectionsClosed.Add(1)
			return
		case <-ticker.C:
			if msgCount >= *messagesPerConn {
				stats.connectionsClosed.Add(1)
				return
			}

			// Send a test message
			msg := &pb.UpstreamMessage{
				Payload: &pb.UpstreamMessage_Send{
					Send: &pb.SendMessage{
						TargetTopic: fmt.Sprintf("ta::loadtest::worker::%d", rand.Intn(10)),
						Payload:     []byte(fmt.Sprintf("Test message %d from agent %d", msgCount, agentID)),
						MessageType: pb.MessageType_CHAT,
					},
				},
			}

			if err := stream.Send(msg); err != nil {
				logging.Logger.Error().Err(err).Int("agent_id", agentID).Msg("failed to send message")
				stats.errors.Add(1)
				return
			}

			stats.messagesSent.Add(1)
			msgCount++
		}
	}
}

func main() {
	flag.Parse()

	// Initialize structured logger
	logging.Init("info")

	logging.Logger.Info().Int("connections", *numConnections).Dur("duration", *duration).Msg("starting load test")
	logging.Logger.Info().Int("messages_per_conn", *messagesPerConn).Msg("messages per connection")
	logging.Logger.Info().Int("total_target", (*numConnections)*(*messagesPerConn)).Msg("target total messages")

	stats := &LoadTestStats{
		startTime: time.Now(),
	}

	// Force GC before starting
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var wg sync.WaitGroup

	// Spawn connections in waves to avoid overwhelming the gateway
	batchSize := 10
	for i := 0; i < *numConnections; i++ {
		wg.Add(1)
		go simulateAgent(ctx, i, stats, &wg)

		// Small delay between batches
		if (i+1)%batchSize == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Wait for test to complete
	wg.Wait()

	// Force GC after test
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	stats.Print()

	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			logging.Logger.Error().Err(err).Msg("failed to create memory profile")
		} else {
			defer f.Close()
			runtime.GC() // get up-to-date statistics
			if err := pprof.WriteHeapProfile(f); err != nil {
				logging.Logger.Error().Err(err).Msg("failed to write memory profile")
			} else {
				logging.Logger.Info().Str("file", *memProfile).Msg("memory profile written")
			}
		}
	}
}
