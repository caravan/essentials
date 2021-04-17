package topic_test

import (
	"testing"

	caravan "github.com/caravan/essentials"
	"github.com/caravan/essentials/topic"
	"github.com/stretchr/testify/assert"
)

func TestMustReceive(t *testing.T) {
	as := assert.New(t)
	top := caravan.NewTopic()
	p := top.NewProducer()
	p.Send("hello")

	c := top.NewConsumer()
	as.Equal("hello", topic.MustReceive(c))
	_ = c.Close()

	defer func() {
		as.Errorf(recover().(error), topic.ErrConsumerClosed)
	}()
	topic.MustReceive(c)
}

func TestMustSend(t *testing.T) {
	as := assert.New(t)
	top := caravan.NewTopic()
	p := top.NewProducer()
	_ = p.Close()

	defer func() {
		as.Errorf(recover().(error), topic.ErrProducerClosed)
	}()
	topic.MustSend(p, "hello")
}
