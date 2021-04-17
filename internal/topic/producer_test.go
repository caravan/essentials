package topic_test

import (
	"runtime"
	"testing"
	"time"

	caravan "github.com/caravan/essentials"
	"github.com/caravan/essentials/id"
	"github.com/caravan/essentials/internal/debug"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/essentials/topic/config"
	"github.com/stretchr/testify/assert"
)

func TestProducerClosed(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic()
	p := top.NewProducer()
	as.Nil(p.Close())
	as.Errorf(p.Close(), topic.ErrProducerClosed)

	as.False(p.Send("blah"))
}

func TestProducerGC(t *testing.T) {
	debug.Enable()

	as := assert.New(t)
	top := caravan.NewTopic()
	top.NewProducer()
	runtime.GC()

	errs := make(chan error)
	go func() {
		debug.WithConsumer(func(c topic.Consumer) {
			errs <- topic.MustReceive(c).(error)
		})
	}()
	as.Error(<-errs)
}

func TestProducer(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic(config.Permanent)
	as.NotNil(top)

	p := top.NewProducer()
	c := top.NewConsumer()

	as.NotNil(p)
	as.NotEqual(id.Nil, p.ID())

	p.Send("first value")
	p.Send("second value")
	p.Send("third value")

	time.Sleep(10 * time.Millisecond)
	as.Equal(topic.Length(3), top.Length())
	as.Nil(p.Close())
	as.Nil(c.Close())
}

func TestLateProducer(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic(config.Permanent)
	p := top.NewProducer()

	pc := p.Channel()
	pc <- "first value"

	c := top.NewConsumer()
	cc := c.Channel()
	as.Equal("first value", <-cc)

	done := make(chan bool)

	go func() {
		as.Equal("second value", <-cc)
		as.Nil(c.Close())
		done <- true
	}()

	pc <- "second value"

	<-done
	as.Nil(p.Close())
}

func TestProducerChannel(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic(config.Permanent)
	as.NotNil(top)

	p := top.NewProducer()
	as.NotNil(p)
	as.NotEqual(id.Nil, p.ID())

	pc := p.Channel()
	pc <- "first value"
	pc <- "second value"
	pc <- "third value"

	done := make(chan bool)
	go func() {
		c := top.NewConsumer()
		time.Sleep(10 * time.Millisecond)
		as.Equal(topic.Length(3), top.Length())
		as.Nil(c.Close())
		done <- true
	}()

	<-done
	as.Nil(p.Close())
}

func TestProducerChannelClosed(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic()
	p := top.NewProducer()
	as.Nil(p.Close())
	as.Errorf(p.Close(), topic.ErrProducerClosed)
}
