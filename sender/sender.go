package sender

import (
	"errors"

	"github.com/caravan/essentials/closer"
	"github.com/caravan/essentials/event"
)

type (
	// Sender is a type that is capable of sending a Message via a channel
	Sender interface {
		Send() chan<- event.Event
	}

	// ClosingSender is a Sender that is capable of being closed
	ClosingSender interface {
		closer.Closer
		Sender
	}
)

// Error messages
const (
	// ErrClosed is raised when MustSend is called on a closed Sender
	ErrClosed = "sender is closed"
)

// Send sends an Event to a ClosingSender
func Send(s ClosingSender, e event.Event) (sent bool) {
	select {
	case <-s.IsClosed():
		return false
	case s.Send() <- e:
		return true
	}
}

// MustSend will send to a ClosingSender or panic if it is closed
func MustSend(s ClosingSender, e event.Event) {
	if !Send(s, e) {
		panic(errors.New(ErrClosed))
	}
}
