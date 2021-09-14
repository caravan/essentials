package topic

import (
	"fmt"
	"runtime"

	"github.com/caravan/essentials/closer"
	"github.com/caravan/essentials/event"
	"github.com/caravan/essentials/id"
	"github.com/caravan/essentials/internal/debug"
	"github.com/caravan/essentials/internal/sync/channel"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/essentials/topic/backoff"
)

type consumer struct {
	*cursor
	id      id.ID
	channel chan event.Event
}

// Error messages
const (
	ErrConsumerNotClosed = "consumer finalized without being channel: %s"
)

func makeConsumer(c *cursor, b backoff.Generator) *consumer {
	res := &consumer{
		cursor:  c,
		id:      c.id,
		channel: startConsumer(c, b),
	}

	if debug.IsEnabled() {
		wrap := debug.WrapStackTrace(debug.MsgInstantiationTrace)
		runtime.SetFinalizer(res, consumerDebugFinalizer(wrap))
	}
	return res
}

func (c *consumer) ID() id.ID {
	return c.id
}

func (c *consumer) Receive() <-chan event.Event {
	return c.channel
}

func startConsumer(c *cursor, b backoff.Generator) chan event.Event {
	ch := make(chan event.Event)
	next := b()
	go func() {
		defer func() {
			// probably because the channel was closed
			recover()
		}()
		for {
			select {
			case <-c.IsClosed():
				goto closed
			default:
				if e, ok := c.head(); ok {
					select {
					case <-c.IsClosed():
						goto closed
					case <-channel.Timeout(next()):
						// allow retention policies to kick in while waiting
						// for a channel read to happen
					case ch <- e:
						// advance the cursor and reset the backoff sequence
						c.advance()
						next = b()
					}
				} else {
					// Wait for something to happen
					select {
					case <-c.IsClosed():
						goto closed
					case <-channel.Timeout(next()):
					case <-c.ready.Wait():
					}
				}
			}
		}
	closed:
		close(ch)
	}()
	return ch
}

func consumerDebugFinalizer(wrap debug.ErrorWrapper) func(c *consumer) {
	return func(c *consumer) {
		if !closer.IsClosed(c) {
			debug.WithProducer(func(dp topic.Producer) {
				err := fmt.Errorf(ErrConsumerNotClosed, c.id)
				dp.Send() <- wrap(err)
			})
		}
	}
}
