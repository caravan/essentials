package message

import (
	"errors"

	"github.com/caravan/essentials/closer"
)

type (
	// Sender is a type that is capable of sending a Message via a channel
	Sender interface {
		Send() chan<- Message
	}

	// ClosingSender is a Sender that is capable of being closed
	ClosingSender interface {
		closer.Closer
		Sender
	}
)

// Error messages
const (
	// ErrSenderClosed is raised when MustSend is called on a closed Sender
	ErrSenderClosed = "sender is closed"
)

// Send sends an Event to a ClosingSender
func Send(s ClosingSender, m Message) (sent bool) {
	select {
	case <-s.IsClosed():
		return false
	default:
		s.Send() <- m
		return true
	}
}

// MustSend will send to a ClosingSender or panic if it is closed
func MustSend(s ClosingSender, m Message) {
	if !Send(s, m) {
		panic(errors.New(ErrSenderClosed))
	}
}
