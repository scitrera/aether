package orchestration

// MemoryQueueCloser is a no-op closer for lite mode where AMQP queues are not used.
type MemoryQueueCloser struct{}

// NewMemoryQueueCloser returns a new MemoryQueueCloser.
func NewMemoryQueueCloser() *MemoryQueueCloser { return &MemoryQueueCloser{} }

// Close is a no-op; satisfies io.Closer.
func (m *MemoryQueueCloser) Close() error { return nil }
