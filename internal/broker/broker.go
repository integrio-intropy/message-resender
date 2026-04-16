package broker

import (
	"context"
	"time"
)

// QueueInfo identifies a queue whose dead-letter queue should be drained.
type QueueInfo struct {
	// QueueName is the origin queue to republish messages to.
	QueueName string
	// DLQOverride, if non-empty, is used as the DLQ name directly,
	// bypassing the broker's default DLQ name derivation.
	DLQOverride string
}

// DrainResult holds the outcome of a single drain run for one queue.
type DrainResult struct {
	Processed int64
	Succeeded int64
	Failed    int64
}

// Broker abstracts a message broker capable of draining dead-letter queues.
type Broker interface {
	// Drain drains the dead-letter queue for the given queue, republishing
	// eligible messages to the origin queue. runStart is used for boundary
	// checks — messages dead-lettered at or after runStart are left in place.
	Drain(ctx context.Context, queue QueueInfo, runStart time.Time) (DrainResult, error)

	// Close releases broker resources (connections, channels, etc.).
	Close() error
}
