package topic_test

import (
	"runtime"
	"testing"

	caravan "github.com/caravan/essentials"
	"github.com/caravan/essentials/internal/debug"
	"github.com/caravan/essentials/topic"
	"github.com/stretchr/testify/assert"

	_topic "github.com/caravan/essentials/internal/topic"
)

func TestDebugProducerClose(t *testing.T) {
	as := assert.New(t)
	debug.Enable()

	debug.WithConsumer(func(c topic.Consumer) {
		top := caravan.NewTopic()
		i := top.NewProducer().ID()
		runtime.GC()

		errs := c.Channel()
		as.Errorf((<-errs).(error), _topic.ErrProducerNotClosed, i)
	})
}

func TestDebugConsumerClose(t *testing.T) {
	as := assert.New(t)
	debug.Enable()

	debug.WithConsumer(func(c topic.Consumer) {
		top := caravan.NewTopic()
		i := top.NewConsumer().ID()
		runtime.GC()

		errs := c.Channel()
		as.Errorf((<-errs).(error), _topic.ErrConsumerNotClosed, i)
	})
}
