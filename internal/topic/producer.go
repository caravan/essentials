package topic

import (
	"fmt"
	"runtime"

	"github.com/caravan/essentials/closer"
	"github.com/caravan/essentials/id"
	"github.com/caravan/essentials/internal/debug"
)

type producer[Msg any] struct {
	closer.Closer
	id      id.ID
	topic   *Topic[Msg]
	channel chan Msg
}

// Error messages
const (
	ErrProducerNotClosed = "producer finalized without being closed: %s"
)

func makeProducer[Msg any](t *Topic[Msg]) *producer[Msg] {
	ch := startProducer(t)
	res := &producer[Msg]{
		id:      id.New(),
		topic:   t,
		channel: ch,
		Closer: makeCloser(func() {
			close(ch)
		}),
	}

	if debug.IsEnabled() {
		wrap := debug.WrapStackTrace(debug.MsgInstantiationTrace)
		runtime.SetFinalizer(res, producerDebugFinalizer[Msg](wrap))
	}
	return res
}

func (p *producer[_]) ID() id.ID {
	return p.id
}

func (p *producer[Msg]) Send() chan<- Msg {
	return p.channel
}

func startProducer[Msg any](t *Topic[Msg]) chan Msg {
	ch := make(chan Msg)
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

func producerDebugFinalizer[Msg any](
	wrap debug.ErrorWrapper,
) func(*producer[Msg]) {
	return func(p *producer[Msg]) {
		if !closer.IsClosed(p) {
			debug.WithProducer(func(dp debug.Producer) {
				err := fmt.Errorf(ErrProducerNotClosed, p.id)
				dp.Send() <- wrap(err)
			})
		}
	}
}
