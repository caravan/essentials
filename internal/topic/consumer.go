package topic

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/caravan/essentials/id"
	"github.com/caravan/essentials/internal/debug"
	"github.com/caravan/essentials/internal/sync/channel"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/essentials/topic/backoff"
)

type consumer struct {
	sync.Mutex
	id      id.ID
	cursor  *cursor
	channel chan topic.Event
}

// Error messages
const (
	ErrConsumerNotClosed = "consumer finalized without being closed: %s"
)

func makeConsumer(c *cursor, b backoff.Generator) *consumer {
	res := &consumer{
		id:      c.id,
		cursor:  c,
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

func (c *consumer) Poll(d time.Duration) (topic.Event, bool) {
	select {
	case <-channel.Timeout(d):
		return nil, false
	case e, ok := <-c.channel:
		return e, ok
	}
}

func (c *consumer) Receive() (topic.Event, bool) {
	res, ok := <-c.channel
	return res, ok
}

func (c *consumer) Channel() <-chan topic.Event {
	return c.channel
}

func (c *consumer) Close() error {
	c.Lock()
	defer c.Unlock()
	if c.isClosed() {
		return errors.New(topic.ErrConsumerClosed)
	}
	c.cursor.close()
	close(c.channel)
	return nil
}

func (c *consumer) IsClosed() bool {
	c.Lock()
	defer c.Unlock()
	return c.isClosed()
}

func (c *consumer) isClosed() bool {
	return c.cursor.isClosed()
}

func startConsumer(c *cursor, b backoff.Generator) chan topic.Event {
	ch := make(chan topic.Event)
	next := b()
	go func() {
		defer func() {
			// probably because the channel was closed
			recover()
		}()
		for !c.isClosed() {
			if e, ok := c.head(); ok {
				select {
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
				case <-channel.Timeout(next()):
				case <-c.ready.Wait():
				}
			}
		}
	}()
	return ch
}

func consumerDebugFinalizer(wrap debug.ErrorWrapper) func(c *consumer) {
	return func(c *consumer) {
		if !c.IsClosed() {
			debug.WithProducer(func(dp topic.Producer) {
				err := fmt.Errorf(ErrConsumerNotClosed, c.id)
				dp.Channel() <- wrap(err)
			})
		}
	}
}
