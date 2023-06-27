package topic

import (
	"github.com/caravan/essentials/id"
	"github.com/caravan/essentials/message"
)

type (
	// Length is the potential size of a Topic stream
	Length uint64

	// Topic is where you put your stuff. They are implemented as a
	// first-in-first-out (FIFO) Log.
	Topic[Msg any] interface {
		// Length returns the current virtual size of the Topic
		Length() Length

		// NewProducer returns a new Producer for this Topic
		NewProducer() Producer[Msg]

		// NewConsumer returns a new Consumer for this Topic
		NewConsumer() Consumer[Msg]
	}

	// Identified is any resource that can be uniquely identified
	Identified interface {
		// ID returns the identifier for this resource
		ID() id.ID
	}

	// Producer exposes a way to push messages to its associated Topic.
	// messages pushed to the Topic are capable of being independently
	// received by all Consumers
	Producer[Msg any] interface {
		message.ClosingSender[Msg]
		Identified
	}

	// Consumer exposes a way to receive messages from its associated Topic.
	// Each Consumer created independently tracks its own position within
	// the Topic
	Consumer[Msg any] interface {
		message.ClosingReceiver[Msg]
		Identified
	}
)

// Error messages
const (
	ErrConsumerNotClosed = "consumer finalized without being closed: %s"
	ErrProducerNotClosed = "producer finalized without being closed: %s"
)
