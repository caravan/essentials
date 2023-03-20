package message

import "time"

type (
	Typed[Msg any] interface {
		Poll(r Receiver[Msg], d time.Duration) (Msg, bool)
		Receive(r Receiver[Msg]) (Msg, bool)
		MustReceive(r Receiver[Msg]) Msg

		Send(s ClosingSender[Msg], m Msg) bool
		MustSend(s ClosingSender[Msg], m Msg)
	}

	typed[Msg any] struct{}
)

func Of[Msg any]() Typed[Msg] {
	return typed[Msg]{}
}

func (typed[Msg]) Poll(r Receiver[Msg], d time.Duration) (Msg, bool) {
	return Poll[Msg](r, d)
}

func (typed[Msg]) Receive(r Receiver[Msg]) (Msg, bool) {
	return Receive[Msg](r)
}

func (t typed[Msg]) MustReceive(r Receiver[Msg]) Msg {
	return MustReceive[Msg](r)
}

func (typed[Msg]) Send(s ClosingSender[Msg], m Msg) bool {
	return Send[Msg](s, m)
}

func (t typed[Msg]) MustSend(s ClosingSender[Msg], m Msg) {
	MustSend[Msg](s, m)
}
