package orchestration

// NoopQueueCloser is a no-op closer for lite mode where AMQP queues are not used.
// Close() returns nil; it satisfies io.Closer as a placeholder for the QueueCloser
// slot in OrchestrationServices when there is no real queue connection to close.
type NoopQueueCloser struct{}

// NewNoopQueueCloser returns a new NoopQueueCloser.
func NewNoopQueueCloser() *NoopQueueCloser { return &NoopQueueCloser{} }

// Close is a no-op; satisfies io.Closer.
func (m *NoopQueueCloser) Close() error { return nil }
