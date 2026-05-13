package orchestration

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/models"
)

// OrchestratedTaskPayload represents a task for agent startup
type OrchestratedTaskPayload struct {
	TaskID               string                 `json:"task_id"`
	TargetImplementation string                 `json:"target_implementation"`
	Workspace            string                 `json:"workspace"`
	Profile              string                 `json:"profile"`       // Orchestration profile (e.g., "kubernetes", "docker")
	LaunchParams         map[string]interface{} `json:"launch_params"` // Full launch parameters
	Metadata             map[string]interface{} `json:"metadata,omitempty"`
}

// OrchestratedQueueManager manages RabbitMQ queues for orchestrated agent startup
type OrchestratedQueueManager struct {
	mu      sync.Mutex
	conn    *amqp.Connection
	channel *amqp.Channel
	amqpURL string
}

// NewOrchestratedQueueManager creates a new orchestrated queue manager
func NewOrchestratedQueueManager(amqpURL string) (*OrchestratedQueueManager, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	return &OrchestratedQueueManager{
		conn:    conn,
		channel: ch,
		amqpURL: amqpURL,
	}, nil
}

// ensureConnection checks if the AMQP connection is alive and reconnects if needed.
func (oqm *OrchestratedQueueManager) ensureConnection() error {
	oqm.mu.Lock()
	defer oqm.mu.Unlock()

	if oqm.channel != nil && !oqm.channel.IsClosed() {
		return nil
	}

	// Connection is dead, try to reconnect
	logging.Logger.Warn().Msg("AMQP connection lost, attempting to reconnect")

	conn, err := amqp.Dial(oqm.amqpURL)
	if err != nil {
		return fmt.Errorf("failed to reconnect to AMQP: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to open channel on reconnected AMQP: %w", err)
	}

	// Close old connection if it exists
	if oqm.conn != nil {
		oqm.conn.Close() // ignore error on old connection
	}

	oqm.conn = conn
	oqm.channel = ch
	logging.Logger.Info().Msg("AMQP connection re-established")
	return nil
}

// Close closes the connection to RabbitMQ
func (oqm *OrchestratedQueueManager) Close() error {
	if oqm.channel != nil {
		oqm.channel.Close()
	}
	if oqm.conn != nil {
		return oqm.conn.Close()
	}
	return nil
}

// DeclareOrchestratedQueue declares a queue for orchestrated tasks
// Queue name format: queue:orchestrated:{workspace}
func (oqm *OrchestratedQueueManager) DeclareOrchestratedQueue(workspace string) error {
	if err := oqm.ensureConnection(); err != nil {
		return fmt.Errorf("failed to ensure AMQP connection: %w", err)
	}

	queueName := oqm.getQueueName(workspace)

	_, err := oqm.channel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		amqp.Table{
			"x-message-ttl": int32(86400000), // 24 hour TTL for unclaimed tasks
		},
	)

	if err != nil {
		return fmt.Errorf("failed to declare queue %s: %w", queueName, err)
	}

	logging.Logger.Info().Str("queue", queueName).Msg("declared orchestrated queue")
	return nil
}

// PublishOrchestratedTask publishes an orchestrated task to the appropriate workspace queue
func (oqm *OrchestratedQueueManager) PublishOrchestratedTask(
	ctx context.Context,
	payload *OrchestratedTaskPayload,
) error {
	queueName := oqm.getQueueName(payload.Workspace)

	// Ensure queue exists
	if err := oqm.DeclareOrchestratedQueue(payload.Workspace); err != nil {
		return err
	}

	// Marshal payload
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Publish to queue
	err = oqm.channel.PublishWithContext(
		ctx,
		"",        // exchange (default)
		queueName, // routing key (queue name)
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
			Headers: amqp.Table{
				"task_id":               payload.TaskID,
				"target_implementation": payload.TargetImplementation,
				"workspace":             payload.Workspace,
			},
		},
	)

	if err != nil {
		return fmt.Errorf("failed to publish to queue %s: %w", queueName, err)
	}

	logging.Logger.Info().Str("task_id", payload.TaskID).Str("queue", queueName).Msg("published orchestrated task")
	return nil
}

// ConsumeOrchestratedTasks starts consuming orchestrated tasks from a workspace queue
// Returns a channel of OrchestratedTaskPayload and error channel
func (oqm *OrchestratedQueueManager) ConsumeOrchestratedTasks(
	workspace string,
	consumerID string,
) (<-chan *OrchestratedTaskPayload, <-chan error, error) {
	queueName := oqm.getQueueName(workspace)

	// Ensure queue exists
	if err := oqm.DeclareOrchestratedQueue(workspace); err != nil {
		return nil, nil, err
	}

	// Start consuming
	deliveries, err := oqm.channel.Consume(
		queueName,  // queue
		consumerID, // consumer tag
		false,      // auto-ack (manual ack for reliability)
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to consume from queue %s: %w", queueName, err)
	}

	taskChan := make(chan *OrchestratedTaskPayload)
	errChan := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logging.Logger.Error().Interface("panic", r).Str("stack", string(debug.Stack())).Str("goroutine", "taskConsumer").Msg("recovered from panic in background goroutine")
			}
		}()
		defer close(taskChan)
		defer close(errChan)

		for delivery := range deliveries {
			var payload OrchestratedTaskPayload
			if err := json.Unmarshal(delivery.Body, &payload); err != nil {
				logging.Logger.Error().Err(err).Msg("failed to unmarshal task payload")
				if nackErr := delivery.Nack(false, true); nackErr != nil { // Requeue
					logging.Logger.Warn().Err(nackErr).Msg("failed to nack undecodable task; channel may close on next delivery")
				}
				continue
			}

			// Send to task channel
			taskChan <- &payload

			// Auto-ack after processing.
			// Note: In production, orchestrator should manually ack after agent starts.
			if ackErr := delivery.Ack(false); ackErr != nil {
				logging.Logger.Warn().Err(ackErr).Msg("failed to ack delivered task; redelivery may occur")
			}
		}
	}()

	logging.Logger.Info().Str("queue", queueName).Msg("started consuming orchestrated tasks")
	return taskChan, errChan, nil
}

// GetQueueDepth returns the number of pending messages in a workspace queue
func (oqm *OrchestratedQueueManager) GetQueueDepth(workspace string) (int, error) {
	if err := oqm.ensureConnection(); err != nil {
		return 0, fmt.Errorf("failed to ensure AMQP connection: %w", err)
	}

	queueName := oqm.getQueueName(workspace)

	// QueueInspect was deprecated in amqp091-go in favor of a passive
	// declare that returns the same Queue struct. Per amqp091-go docs:
	// "QueueDeclarePassive is the supported way to test for the existence
	// of a queue and retrieve its current depth." The flags here mirror
	// DeclareOrchestratedQueue (durable/autoDelete/exclusive/no-wait, and
	// the 24h x-message-ttl table arg) so brokers do not raise a
	// PRECONDITION_FAILED on a mismatched re-declare.
	queue, err := oqm.channel.QueueDeclarePassive(queueName, true, false, false, false, amqp.Table{
		"x-message-ttl": int32(86400000),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to inspect queue %s: %w", queueName, err)
	}

	return queue.Messages, nil
}

// PurgeQueue removes all messages from a workspace queue
func (oqm *OrchestratedQueueManager) PurgeQueue(workspace string) error {
	if err := oqm.ensureConnection(); err != nil {
		return fmt.Errorf("failed to ensure AMQP connection: %w", err)
	}

	queueName := oqm.getQueueName(workspace)

	_, err := oqm.channel.QueuePurge(queueName, false)
	if err != nil {
		return fmt.Errorf("failed to purge queue %s: %w", queueName, err)
	}

	logging.Logger.Info().Str("queue", queueName).Msg("purged orchestrated queue")
	return nil
}

// getQueueName returns the RabbitMQ queue name for a workspace
// Format: queue:orchestrated:{workspace}
// Special case: _system workspace for global orchestrated tasks
func (oqm *OrchestratedQueueManager) getQueueName(workspace string) string {
	if workspace == "" {
		workspace = models.SystemWorkspace
	}
	return fmt.Sprintf("queue:orchestrated:%s", workspace)
}

// ListOrchestratedQueues returns all orchestrated queues (for monitoring)
func (oqm *OrchestratedQueueManager) ListOrchestratedQueues() ([]string, error) {
	// Note: RabbitMQ doesn't have a direct API to list queues via AMQP
	// This would typically be done via the Management API
	// For now, return empty slice - can be extended with HTTP API calls
	return []string{}, nil
}
