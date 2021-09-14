package topic_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/caravan/essentials/closer"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/id"
	"github.com/caravan/essentials/internal/debug"
	"github.com/caravan/essentials/receiver"
	"github.com/caravan/essentials/sender"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/essentials/topic/config"
	"github.com/stretchr/testify/assert"
)

func TestConsumerClosed(t *testing.T) {
	as := assert.New(t)

	top := essentials.NewTopic()
	c := top.NewConsumer()
	c.Close()

	as.True(closer.IsClosed(c))
}

func TestConsumerGC(t *testing.T) {
	debug.Enable()

	as := assert.New(t)
	top := essentials.NewTopic()
	top.NewConsumer()
	runtime.GC()

	errs := make(chan error)
	go func() {
		debug.WithConsumer(func(c topic.Consumer) {
			e, _ := receiver.Receive(c)
			errs <- e.(error)
		})
	}()
	as.Error(<-errs)
}

func TestEmptyConsumer(t *testing.T) {
	as := assert.New(t)
	top := essentials.NewTopic(config.Permanent)
	c := top.NewConsumer()
	e, ok := receiver.Poll(c, 0)
	as.Nil(e)
	as.False(ok)
	c.Close()
}

func TestSingleConsumer(t *testing.T) {
	as := assert.New(t)

	top := essentials.NewTopic(config.Permanent)
	as.NotNil(top)

	p := top.NewProducer()
	as.NotNil(p)

	sender.Send(p, "first value")
	sender.Send(p, "second value")
	sender.Send(p, "third value")

	c := top.NewConsumer()
	as.NotNil(c)
	as.NotEqual(id.Nil, c.ID())

	as.Equal("first value", receiver.MustReceive(c))
	as.Equal("second value", receiver.MustReceive(c))
	as.Equal("third value", receiver.MustReceive(c))

	p.Close()
	c.Close()
}

func TestMultiConsumer(t *testing.T) {
	as := assert.New(t)

	top := essentials.NewTopic(config.Permanent)
	as.NotNil(top)

	p := top.NewProducer()
	as.NotNil(p)

	sender.Send(p, "first value")
	sender.Send(p, "second value")
	sender.Send(p, "third value")

	c1 := top.NewConsumer()
	c2 := top.NewConsumer()

	as.Equal("first value", receiver.MustReceive(c1))
	as.Equal("second value", receiver.MustReceive(c1))
	as.Equal("first value", receiver.MustReceive(c2))
	as.Equal("second value", receiver.MustReceive(c2))
	as.Equal("third value", receiver.MustReceive(c2))
	as.Equal("third value", receiver.MustReceive(c1))

	p.Close()
	c1.Close()
	c2.Close()
}

func TestLoadedConsumer(t *testing.T) {
	as := assert.New(t)

	top := essentials.NewTopic(config.Permanent)
	p := top.NewProducer()
	for i := 0; i < 10000; i++ {
		sender.Send(p, i)
	}

	done := make(chan bool)

	go func() {
		c := top.NewConsumer()
		for i := 0; i < 10000; i++ {
			as.Equal(i, receiver.MustReceive(c))
		}
		c.Close()
		done <- true
	}()

	<-done
	p.Close()
}

func TestStreamingConsumer(t *testing.T) {
	as := assert.New(t)

	top := essentials.NewTopic(config.Consumed)
	p := top.NewProducer()
	c := top.NewConsumer()

	go func() {
		for i := 0; i < 100000; i++ {
			sender.Send(p, i)
		}
		p.Close()
	}()

	done := make(chan bool)

	go func() {
		for i := 0; i < 100000; i++ {
			as.Equal(i, receiver.MustReceive(c))
		}
		done <- true
	}()

	<-done
	c.Close()
}

func TestConsumerClosedDuringPoll(t *testing.T) {
	as := assert.New(t)

	top := essentials.NewTopic()
	p := top.NewProducer()
	c := top.NewConsumer()

	go func() {
		time.Sleep(100 * time.Millisecond)
		c.Close()
		sender.Send(p, 1) // Consumer should not receive
	}()

	e, ok := receiver.Poll(c, time.Second)
	as.Nil(e)
	as.False(ok)
}

func TestConsumerChannel(t *testing.T) {
	as := assert.New(t)

	top := essentials.NewTopic(config.Permanent)
	as.NotNil(top)

	p := top.NewProducer()
	as.NotNil(p)

	pc := p.Send()
	pc <- "first value"
	pc <- "second value"
	pc <- "third value"
	p.Close()

	c := top.NewConsumer()
	as.NotNil(c)
	as.NotEqual(id.Nil, c.ID())

	cc := c.Receive()
	as.Equal("first value", <-cc)
	as.Equal("second value", <-cc)
	as.Equal("third value", <-cc)
	c.Close()
}

func TestConsumerChannelClosed(t *testing.T) {
	as := assert.New(t)

	top := essentials.NewTopic()
	c := top.NewConsumer()
	ch := c.Receive()
	c.Close()

	_, ok := <-ch
	as.False(ok)
}
