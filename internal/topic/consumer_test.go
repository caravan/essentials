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

func TestConsumerClosed(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic()
	c := top.NewConsumer()
	as.Nil(c.Close())

	as.Errorf(c.Close(), topic.ErrConsumerClosed)
}

func TestConsumerGC(t *testing.T) {
	debug.Enable()

	as := assert.New(t)
	top := caravan.NewTopic()
	top.NewConsumer()
	runtime.GC()

	errs := make(chan error)
	go func() {
		debug.WithConsumer(func(c topic.Consumer) {
			e, _ := c.Receive()
			errs <- e.(error)
		})
	}()
	as.Error(<-errs)
}

func TestEmptyConsumer(t *testing.T) {
	as := assert.New(t)
	top := caravan.NewTopic(config.Permanent)
	c := top.NewConsumer()
	e, ok := c.Poll(0)
	as.Nil(e)
	as.False(ok)
	as.Nil(c.Close())
}

func TestSingleConsumer(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic(config.Permanent)
	as.NotNil(top)

	p := top.NewProducer()
	as.NotNil(p)

	p.Send("first value")
	p.Send("second value")
	p.Send("third value")

	c := top.NewConsumer()
	as.NotNil(c)
	as.NotEqual(id.Nil, c.ID())

	as.Equal("first value", topic.MustReceive(c))
	as.Equal("second value", topic.MustReceive(c))
	as.Equal("third value", topic.MustReceive(c))

	as.Nil(p.Close())
	as.Nil(c.Close())
}

func TestMultiConsumer(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic(config.Permanent)
	as.NotNil(top)

	p := top.NewProducer()
	as.NotNil(p)

	p.Send("first value")
	p.Send("second value")
	p.Send("third value")

	c1 := top.NewConsumer()
	c2 := top.NewConsumer()

	as.Equal("first value", topic.MustReceive(c1))
	as.Equal("second value", topic.MustReceive(c1))
	as.Equal("first value", topic.MustReceive(c2))
	as.Equal("second value", topic.MustReceive(c2))
	as.Equal("third value", topic.MustReceive(c2))
	as.Equal("third value", topic.MustReceive(c1))

	as.Nil(p.Close())
	as.Nil(c1.Close())
	as.Nil(c2.Close())
}

func TestLoadedConsumer(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic(config.Permanent)
	p := top.NewProducer()
	for i := 0; i < 10000; i++ {
		p.Send(i)
	}

	done := make(chan bool)

	go func() {
		c := top.NewConsumer()
		for i := 0; i < 10000; i++ {
			as.Equal(i, topic.MustReceive(c))
		}
		as.Nil(c.Close())
		done <- true
	}()

	<-done
	as.Nil(p.Close())
}

func TestStreamingConsumer(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic(config.Consumed)
	p := top.NewProducer()
	c := top.NewConsumer()

	go func() {
		for i := 0; i < 100000; i++ {
			p.Send(i)
		}
		as.Nil(p.Close())
	}()

	done := make(chan bool)

	go func() {
		for i := 0; i < 100000; i++ {
			as.Equal(i, topic.MustReceive(c))
		}
		done <- true
	}()

	<-done
	as.Nil(c.Close())
}

func TestConsumerClosedDuringPoll(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic()
	p := top.NewProducer()
	c := top.NewConsumer()

	go func() {
		time.Sleep(100 * time.Millisecond)
		as.Nil(c.Close())
		p.Send(1) // Consumer should not receive
	}()

	e, ok := c.Poll(time.Second)
	as.Nil(e)
	as.False(ok)
}

func TestConsumerChannel(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic(config.Permanent)
	as.NotNil(top)

	p := top.NewProducer()
	as.NotNil(p)

	pc := p.Channel()
	pc <- "first value"
	pc <- "second value"
	pc <- "third value"
	as.Nil(p.Close())

	c := top.NewConsumer()
	as.NotNil(c)
	as.NotEqual(id.Nil, c.ID())

	cc := c.Channel()
	as.Equal("first value", <-cc)
	as.Equal("second value", <-cc)
	as.Equal("third value", <-cc)
	as.Nil(c.Close())
}

func TestConsumerChannelClosed(t *testing.T) {
	as := assert.New(t)

	top := caravan.NewTopic()
	c := top.NewConsumer()
	c.Channel()
	as.Nil(c.Close())
	as.Errorf(c.Close(), topic.ErrConsumerClosed)
}
