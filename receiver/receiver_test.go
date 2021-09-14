package receiver_test

import (
	"testing"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/receiver"
	"github.com/caravan/essentials/sender"
	"github.com/stretchr/testify/assert"
)

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
