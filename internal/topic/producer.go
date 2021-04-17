package topic

import (
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/caravan/essentials/id"
	"github.com/caravan/essentials/internal/debug"
	"github.com/caravan/essentials/topic"
)

type producer struct {
	sync.Mutex
	id      id.ID
	topic   *Topic
	channel chan topic.Event
}

// Error messages
const (
	ErrProducerNotClosed = "producer finalized without being closed: %s"
)

func makeProducer(t *Topic) *producer {
	res := &producer{
		id:      id.New(),
		topic:   t,
		channel: startProducer(t),
	}

	if debug.IsEnabled() {
		wrap := debug.WrapStackTrace(debug.MsgInstantiationTrace)
		runtime.SetFinalizer(res, producerDebugFinalizer(wrap))
	}
	return res
}

func (p *producer) ID() id.ID {
	return p.id
}

func (p *producer) Send(e topic.Event) bool {
	p.Lock()
	defer p.Unlock()
	if p.isClosed() {
		return false
	}
	p.channel <- e
	return true
}

func (p *producer) Channel() chan<- topic.Event {
	return p.channel
}

func (p *producer) Close() error {
	p.Lock()
	defer p.Unlock()
	if p.isClosed() {
		return errors.New(topic.ErrProducerClosed)
	}
	p.topic = nil
	close(p.channel)
	return nil
}

func (p *producer) IsClosed() bool {
	p.Lock()
	defer p.Unlock()
	return p.isClosed()
}

func (p *producer) isClosed() bool {
	return p.topic == nil
}

func startProducer(t *Topic) chan topic.Event {
	ch := make(chan topic.Event)
	go func() {
		defer func() {
			// probably because the channel was closed
			recover()
		}()
		for e := range ch {
			t.Put(e)
		}
	}()
	return ch
}

func producerDebugFinalizer(wrap debug.ErrorWrapper) func(*producer) {
	return func(p *producer) {
		if !p.IsClosed() {
			debug.WithProducer(func(dp topic.Producer) {
				err := fmt.Errorf(ErrProducerNotClosed, p.id)
				dp.Channel() <- wrap(err)
			})
		}
	}
}
