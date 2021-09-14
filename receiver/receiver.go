package receiver

import (
	"errors"
	"time"

	"github.com/caravan/essentials/closer"
	"github.com/caravan/essentials/event"
	"github.com/caravan/essentials/internal/sync/channel"
)

type (
	// Receiver is a type that is capable of receiving a Message via a channel
	Receiver interface {
		Receive() <-chan event.Event
	}

	// ClosingReceiver is a Receiver that is capable of being closed
	ClosingReceiver interface {
		closer.Closer
		Receiver
	}
)

// Error messages
const (
	// ErrClosed is raised when MustReceive is called on a closed Receiver
	ErrClosed = "receiver is closed"
)

// Poll will wait up until the specified Duration for an Event to possibly be
// returned, advancing the ClosingReceiver's Cursor upon success
func Poll(r ClosingReceiver, d time.Duration) (event.Event, bool) {
	select {
	case <-channel.Timeout(d):
		return nil, false
	case e, ok := <-r.Receive():
		return e, ok
	}
}

// Receive returns the next Event, blocking indefinitely, and advancing the
// ClosingReceiver's Cursor upon completion
func Receive(r ClosingReceiver) (event.Event, bool) {
	e, ok := <-r.Receive()
	return e, ok
}

// MustReceive will receive from a ClosingReceiver or panic if it is closed
func MustReceive(r ClosingReceiver) event.Event {
	if r, ok := Receive(r); ok {
		return r
	}
	panic(errors.New(ErrClosed))
}
