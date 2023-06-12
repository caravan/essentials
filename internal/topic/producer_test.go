package topic_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/closer"
	"github.com/caravan/essentials/id"
	"github.com/caravan/essentials/internal/debug"
	"github.com/caravan/essentials/message"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/essentials/topic/config"
	"github.com/stretchr/testify/assert"
)

func TestProducerClosed(t *testing.T) {
	as := assert.New(t)

	top := essentials.NewTopic[any]()
	p := top.NewProducer()

	p.Close()
	as.True(closer.IsClosed(p))
	as.False(message.Send[any](p, "blah"))

	p.Close()
	as.True(closer.IsClosed(p)) // still closed
}

func TestProducerGC(t *testing.T) {
	debug.Enable()

	as := assert.New(t)
	top := essentials.NewTopic[any]()
	top.NewProducer()
	runtime.GC()

	errs := make(chan error)
	go func() {
		debug.WithConsumer(func(c debug.Consumer) {
			errs <- message.MustReceive[error](c)
		})
	}()
	as.Error(<-errs)
}

func TestProducer(t *testing.T) {
	as := assert.New(t)

	top := essentials.NewTopic[any](config.Permanent)
	as.NotNil(top)

	p := top.NewProducer()
	c := top.NewConsumer()
	msg := essentials.Of[any]()

	as.NotNil(p)
	as.NotEqual(id.Nil, p.ID())

	msg.Send(p, "first value")
	msg.Send(p, "second value")
	msg.Send(p, "third value")

	time.Sleep(10 * time.Millisecond)
	as.Equal(topic.Length(3), top.Length())
	p.Close()
	c.Close()
}

func TestLateProducer(t *testing.T) {
	as := assert.New(t)

	top := essentials.NewTopic[any]()
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

	top := essentials.NewTopic[any](config.Permanent)
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

	top := essentials.NewTopic[any]()
	p := top.NewProducer()
	ch := p.Send()
	p.Close()

	defer func() {
		as.NotNil(recover())
	}()

	ch <- "hello"
}
