package message_test

import (
	"testing"
	"time"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/message"
	"github.com/stretchr/testify/assert"
)

func TestTypedPoll(t *testing.T) {
	as := assert.New(t)
	top := essentials.NewTopic[any]()
	p := top.NewProducer()
	msg := message.Of[any]()
	msg.Send(p, "hello")

	c := top.NewConsumer()
	e, ok := msg.Poll(c, time.Millisecond)
	as.Equal("hello", e)
	as.True(ok)

	e, ok = msg.Poll(c, time.Millisecond)
	as.Nil(e)
	as.False(ok)
	c.Close()
}

func TestTypedReceive(t *testing.T) {
	as := assert.New(t)
	top := essentials.NewTopic[any]()
	p := top.NewProducer()
	msg := message.Of[any]()
	msg.Send(p, "hello")

	c := top.NewConsumer()
	e, ok := msg.Receive(c)
	as.Equal("hello", e)
	as.True(ok)
	c.Close()
}
func TestTypedMustReceive(t *testing.T) {
	as := assert.New(t)
	top := essentials.NewTopic[any]()
	p := top.NewProducer()
	msg := message.Of[any]()
	msg.Send(p, "hello")

	c := top.NewConsumer()
	as.Equal("hello", msg.MustReceive(c))
	c.Close()

	defer func() {
		as.Errorf(recover().(error), message.ErrReceiverClosed)
	}()
	msg.MustReceive(c)
}

func TestTypedMustSend(t *testing.T) {
	as := assert.New(t)
	top := essentials.NewTopic[any]()
	p := top.NewProducer()
	msg := message.Of[any]()

	msg.MustSend(p, "hello")
	p.Close()

	defer func() {
		as.Errorf(recover().(error), message.ErrSenderClosed)
	}()
	msg.MustSend(p, "explode")
}
