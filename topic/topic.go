package topic

import (
	"errors"
	"time"

	"github.com/caravan/essentials/id"
)

type (
	// Offset is a location within a Topic stream
	Offset uint64

	// Length is the potential size of a Topic stream
	Length uint64

	// Event is a value that is produced and consumed by Topics. It is a
	// placeholder for what will eventually be a generic
	Event interface{}

	// Closer is a value whose underlying resources must be explicitly
	// closed
	Closer interface {
		Close() error
		IsClosed() bool
	}

	// Topic is where you put your stuff. They are implemented as a
	// first-in-first-out (FIFO) Log.
	Topic interface {
		// Length returns the current virtual size of the Topic
		Length() Length

		// NewProducer returns a new Producer for this Topic
		NewProducer() Producer

		// NewConsumer returns a new Consumer for this Topic
		NewConsumer() Consumer
	}

	// Producer exposes a way to push Events to its associated Topic.
	// Events pushed to the Topic are capable of being independently
	// received by all Consumers
	Producer interface {
		Closer

		// ID returns the identifier for this Producer
		ID() id.ID

		// Send sends an Event to the Producer's Topic
		Send(Event) bool

		// Channel returns the channel associated with this Producer
		Channel() chan<- Event
	}

	// Consumer exposes a way to receive Events from its associated Topic.
	// Each Consumer created independently tracks its own position within
	// the Topic
	Consumer interface {
		Closer

		// ID returns the identifier for this Consumer
		ID() id.ID

		// Receive returns the next Event, blocking indefinitely, and
		// advancing the Consumer's Cursor upon completion
		Receive() (Event, bool)

		// Poll will wait up until the specified Duration for an Event to
		// possibly be returned, advancing the Consumer's Cursor upon
		// success
		Poll(time.Duration) (Event, bool)

		// Channel returns the channel associated with this Consumer
		Channel() <-chan Event
	}
)

// Error messages
const (
	ErrConsumerClosed = "consumer is closed"
	ErrProducerClosed = "producer is closed"
)

// MustReceive will receives from a Consumer or panic if it is closed
func MustReceive(c Consumer) Event {
	if r, ok := c.Receive(); ok {
		return r
	}
	panic(errors.New(ErrConsumerClosed))
}

// MustSend will send to a Producer or panic if it is closed
func MustSend(p Producer, e Event) {
	if !p.Send(e) {
		panic(errors.New(ErrProducerClosed))
	}
}

// Next returns the next logical Offset. Should Offsets ever become
// something other than integers, this will spare consuming code
func (o Offset) Next() Offset {
	return o + 1
}
