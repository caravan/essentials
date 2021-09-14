package receiver_test

import (
	"testing"
	"time"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/receiver"
	"github.com/caravan/essentials/sender"
	"github.com/stretchr/testify/assert"
)

func TestPoll(t *testing.T) {
	as := assert.New(t)
	top := essentials.NewTopic()
	p := top.NewProducer()
	sender.Send(p, "hello")

	c := top.NewConsumer()
	e, ok := receiver.Poll(c, time.Millisecond)
	as.Equal("hello", e)
	as.True(ok)

	e, ok = receiver.Poll(c, time.Millisecond)
	as.Nil(e)
	as.False(ok)
	c.Close()
}

func TestMustReceive(t *testing.T) {
	as := assert.New(t)
	top := essentials.NewTopic()
	p := top.NewProducer()
	sender.Send(p, "hello")

	c := top.NewConsumer()
	as.Equal("hello", receiver.MustReceive(c))
	c.Close()

	defer func() {
		as.Errorf(recover().(error), receiver.ErrClosed)
	}()
	receiver.MustReceive(c)
}
