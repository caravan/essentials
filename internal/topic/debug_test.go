package topic_test

import (
	"runtime"
	"testing"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/internal/debug"
	"github.com/stretchr/testify/assert"

	_topic "github.com/caravan/essentials/internal/topic"
)

func TestDebugProducerClose(t *testing.T) {
	as := assert.New(t)
	debug.Enable()

	debug.WithConsumer(func(c debug.Consumer) {
		top := essentials.NewTopic[any]()
		i := top.NewProducer().ID()
		runtime.GC()

		errs := c.Receive()
		as.Errorf(<-errs, _topic.ErrProducerNotClosed, i)
	})
}

func TestDebugConsumerClose(t *testing.T) {
	as := assert.New(t)
	debug.Enable()

	debug.WithConsumer(func(c debug.Consumer) {
		top := essentials.NewTopic[any]()
		i := top.NewConsumer().ID()
		runtime.GC()

		errs := c.Receive()
		as.Errorf(<-errs, _topic.ErrConsumerNotClosed, i)
	})
}
