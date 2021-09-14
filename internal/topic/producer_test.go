package topic_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/closer"
	"github.com/caravan/essentials/id"
	"github.com/caravan/essentials/internal/debug"
	"github.com/caravan/essentials/receiver"
	"github.com/caravan/essentials/sender"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/essentials/topic/config"
	"github.com/stretchr/testify/assert"
)

func TestProducerClosed(t *testing.T) {
	as := assert.New(t)

	top := essentials.NewTopic()
	p := top.NewProducer()

	p.Close()
	as.True(closer.IsClosed(p))
	as.False(sender.Send(p, "blah"))

	p.Close()
	as.True(closer.IsClosed(p)) // still closed
}

func TestProducerGC(t *testing.T) {
	debug.Enable()

	as := assert.New(t)
	top := essentials.NewTopic()
	top.NewProducer()
	runtime.GC()

	errs := make(chan error)
	go func() {
		debug.WithConsumer(func(c topic.Consumer) {
			errs <- receiver.MustReceive(c).(error)
		})
	}()
	as.Error(<-errs)
}

func TestProducer(t *testing.T) {
	as := assert.New(t)

	top := essentials.NewTopic(config.Permanent)
	as.NotNil(top)

	p := top.NewProducer()
	c := top.NewConsumer()

	as.NotNil(p)
	as.NotEqual(id.Nil, p.ID())

	sender.Send(p, "first value")
	sender.Send(p, "second value")
	sender.Send(p, "third value")

	time.Sleep(10 * time.Millisecond)
	as.Equal(topic.Length(3), top.Length())
	p.Close()
	c.Close()
}

func TestLateProducer(t *testing.T) {
	as := assert.New(t)

	top := essentials.NewTopic(config.Permanent)
	p := top.NewProducer()

	pc := p.Send()
	pc <- "first value"

	c := top.NewConsumer()
	cc := c.Receive()
	as.Equal("first value", <-cc)

	done := make(chan bool)

	go func() {
		as.Equal("second value", <-cc)
		c.Close()
		done <- true
	}()

	pc <- "second value"

	<-done
	p.Close()
}

func TestProducerChannel(t *testing.T) {
	as := assert.New(t)

	top := essentials.NewTopic(config.Permanent)
	as.NotNil(top)

	p := top.NewProducer()
	as.NotNil(p)
	as.NotEqual(id.Nil, p.ID())

	pc := p.Send()
	pc <- "first value"
	pc <- "second value"
	pc <- "third value"

	done := make(chan bool)
	go func() {
		c := top.NewConsumer()
		time.Sleep(10 * time.Millisecond)
		as.Equal(topic.Length(3), top.Length())
		c.Close()
		done <- true
	}()

	<-done
	p.Close()
}

func TestProducerChannelClosed(t *testing.T) {
	as := assert.New(t)

	top := essentials.NewTopic()
	p := top.NewProducer()
	ch := p.Send()
	p.Close()

	defer func() {
		as.NotNil(recover())
	}()

	ch <- "hello"
}
