package message_test

import (
	"testing"
	"time"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/message"
	"github.com/stretchr/testify/assert"
)

func TestPoll(t *testing.T) {
	as := assert.New(t)
	top := essentials.NewTopic[any]()
	p := top.NewProducer()
	message.Send[any](p, "hello")

	c := top.NewConsumer()
	e, ok := message.Poll[any](c, time.Millisecond)
	as.Equal("hello", e)
	as.True(ok)

	e, ok = message.Poll[any](c, time.Millisecond)
	as.Nil(e)
	as.False(ok)
	c.Close()
}

func TestMustReceive(t *testing.T) {
	as := assert.New(t)
	top := essentials.NewTopic[any]()
	p := top.NewProducer()
	message.Send[any](p, "hello")

	c := top.NewConsumer()
	as.Equal("hello", message.MustReceive[any](c))
	c.Close()

	defer func() {
		as.Errorf(recover().(error), message.ErrReceiverClosed)
	}()
	message.MustReceive[any](c)
}
