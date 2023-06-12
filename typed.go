package essentials

import (
	"time"

	_topic "github.com/caravan/essentials/internal/topic"
	"github.com/caravan/essentials/message"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/essentials/topic/config"
)

type (
	Typed[Msg any] interface {
		NewTopic(o ...config.Option) topic.Topic[Msg]

		Poll(r message.Receiver[Msg], d time.Duration) (Msg, bool)
		Receive(r message.Receiver[Msg]) (Msg, bool)
		MustReceive(r message.Receiver[Msg]) Msg

		Send(s message.ClosingSender[Msg], m Msg) bool
		MustSend(s message.ClosingSender[Msg], m Msg)
	}

	typed[Msg any] struct{}
)

func Of[Msg any]() Typed[Msg] {
	return typed[Msg]{}
}

func (typed[Msg]) NewTopic(o ...config.Option) topic.Topic[Msg] {
	return _topic.Make[Msg](o...)
}

func (typed[Msg]) Poll(r message.Receiver[Msg], d time.Duration) (Msg, bool) {
	return message.Poll[Msg](r, d)
}

func (typed[Msg]) Receive(r message.Receiver[Msg]) (Msg, bool) {
	return message.Receive[Msg](r)
}

func (t typed[Msg]) MustReceive(r message.Receiver[Msg]) Msg {
	return message.MustReceive[Msg](r)
}

func (typed[Msg]) Send(s message.ClosingSender[Msg], m Msg) bool {
	return message.Send[Msg](s, m)
}

func (t typed[Msg]) MustSend(s message.ClosingSender[Msg], m Msg) {
	message.MustSend[Msg](s, m)
}
