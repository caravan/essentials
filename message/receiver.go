package message

import (
	"errors"
	"time"

	"github.com/caravan/essentials/closer"
	"github.com/caravan/essentials/internal/sync/channel"
)

type (
	// Receiver is a type that is capable of receiving a Message via a channel
	Receiver interface {
		Receive() <-chan Message
	}

	// ClosingReceiver is a Receiver that is capable of being closed
	ClosingReceiver interface {
		closer.Closer
		Receiver
	}
)

// Error messages
const (
	// ErrReceiverClosed is raised when MustReceive is called on a closed Receiver
	ErrReceiverClosed = "receiver is closed"
)

// Poll will wait up until the specified Duration for an Event to possibly be
// returned, advancing the ClosingReceiver's Cursor upon success
func Poll(r ClosingReceiver, d time.Duration) (Message, bool) {
	select {
	case <-channel.Timeout(d):
		return nil, false
	case m, ok := <-r.Receive():
		return m, ok
	}
}

// Receive returns the next Event, blocking indefinitely, and advancing the
// ClosingReceiver's Cursor upon completion
func Receive(r ClosingReceiver) (Message, bool) {
	m, ok := <-r.Receive()
	return m, ok
}

// MustReceive will receive from a ClosingReceiver or panic if it is closed
func MustReceive(r ClosingReceiver) Message {
	if m, ok := Receive(r); ok {
		return m
	}
	panic(errors.New(ErrReceiverClosed))
}
