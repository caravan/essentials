package topic_test

import (
	"runtime"
	"testing"

	"github.com/caravan/essentials/internal/debug"
	"github.com/caravan/essentials/topic"
	"github.com/stretchr/testify/assert"

	internal "github.com/caravan/essentials/internal/topic"
)

func TestDebugProducerClose(t *testing.T) {
	as := assert.New(t)
	debug.Enable()

	debug.WithConsumer(func(c debug.Consumer) {
		top := internal.Make[any]()
		i := top.NewProducer().ID()
		runtime.GC()

		errs := c.Receive()
		as.Errorf(<-errs, topic.ErrProducerNotClosed, i)
	})
}

func TestDebugConsumerClose(t *testing.T) {
	as := assert.New(t)
	debug.Enable()

	debug.WithConsumer(func(c debug.Consumer) {
		top := internal.Make[any]()
		i := top.NewConsumer().ID()
		runtime.GC()

		errs := c.Receive()
		as.Errorf(<-errs, topic.ErrConsumerNotClosed, i)
	})
}
