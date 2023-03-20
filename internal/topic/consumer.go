package topic

import (
	"fmt"
	"runtime"

	"github.com/caravan/essentials/closer"
	"github.com/caravan/essentials/id"
	"github.com/caravan/essentials/internal/debug"
	"github.com/caravan/essentials/internal/sync/channel"
	"github.com/caravan/essentials/topic/backoff"
)

type consumer[Msg any] struct {
	*cursor[Msg]
	id      id.ID
	channel chan Msg
}

// Error messages
const (
	ErrConsumerNotClosed = "consumer finalized without being closed: %s"
)

func makeConsumer[Msg any](c *cursor[Msg], b backoff.Generator) *consumer[Msg] {
	res := &consumer[Msg]{
		cursor:  c,
		id:      c.id,
		channel: startConsumer(c, b),
	}

	if debug.IsEnabled() {
		wrap := debug.WrapStackTrace(debug.MsgInstantiationTrace)
		runtime.SetFinalizer(res, consumerDebugFinalizer[Msg](wrap))
	}
	return res
}

func (c *consumer[_]) ID() id.ID {
	return c.id
}

func (c *consumer[Msg]) Receive() <-chan Msg {
	return c.channel
}

func startConsumer[Msg any](c *cursor[Msg], b backoff.Generator) chan Msg {
	ch := make(chan Msg)
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

func consumerDebugFinalizer[Msg any](
	wrap debug.ErrorWrapper,
) func(c *consumer[Msg]) {
	return func(c *consumer[Msg]) {
		if !closer.IsClosed(c) {
			debug.WithProducer(func(dp debug.Producer) {
				err := fmt.Errorf(ErrConsumerNotClosed, c.id)
				dp.Send() <- wrap(err)
			})
		}
	}
}
