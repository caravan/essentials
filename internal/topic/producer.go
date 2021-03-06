package topic

import (
	"fmt"
	"runtime"

	"github.com/caravan/essentials/closer"
	"github.com/caravan/essentials/id"
	"github.com/caravan/essentials/internal/debug"
	"github.com/caravan/essentials/message"
	"github.com/caravan/essentials/topic"
)

type producer struct {
	closer.Closer
	id      id.ID
	topic   *Topic
	channel chan message.Event
}

// Error messages
const (
	ErrProducerNotClosed = "producer finalized without being closed: %s"
)

func makeProducer(t *Topic) *producer {
	ch := startProducer(t)
	res := &producer{
		id:      id.New(),
		topic:   t,
		channel: ch,
		Closer: makeCloser(func() {
			close(ch)
		}),
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

func (p *producer) Send() chan<- message.Event {
	return p.channel
}

func startProducer(t *Topic) chan message.Event {
	ch := make(chan message.Event)
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
		if !closer.IsClosed(p) {
			debug.WithProducer(func(dp topic.Producer) {
				err := fmt.Errorf(ErrProducerNotClosed, p.id)
				dp.Send() <- wrap(err)
			})
		}
	}
}
