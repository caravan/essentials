package topic

import (
	"github.com/caravan/essentials/closer"
	"github.com/caravan/essentials/id"
	"github.com/caravan/essentials/message"
)

type (
	// Length is the potential size of a Topic stream
	Length uint64

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

	// Identified is any resource that can be uniquely identified
	Identified interface {
		// ID returns the identifier for this resource
		ID() id.ID
	}

	// Producer exposes a way to push Events to its associated Topic.
	// Events pushed to the Topic are capable of being independently
	// received by all Consumers
	Producer interface {
		Identified
		message.Sender
		closer.Closer
	}

	// Consumer exposes a way to receive Events from its associated Topic.
	// Each Consumer created independently tracks its own position within
	// the Topic
	Consumer interface {
		Identified
		message.Receiver
		closer.Closer
	}
)
