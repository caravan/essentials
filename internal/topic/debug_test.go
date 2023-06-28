package topic_test

import (
	"runtime"
	"testing"

	"github.com/caravan/essentials/topic"
	"github.com/stretchr/testify/assert"

	internal "github.com/caravan/essentials/internal/topic"
)

func TestDebugProducerClose(t *testing.T) {
	as := assert.New(t)
	internal.Debug.Enable()

	internal.Debug.WithConsumer(func(c topic.Consumer[error]) {
		top := internal.Make[any]()
		i := top.NewProducer().ID()
		runtime.GC()

		errs := c.Receive()
		as.Errorf(<-errs, topic.ErrProducerNotClosed, i)
	})
}

func TestDebugConsumerClose(t *testing.T) {
	as := assert.New(t)
	internal.Debug.Enable()

	internal.Debug.WithConsumer(func(c topic.Consumer[error]) {
		top := internal.Make[any]()
		i := top.NewConsumer().ID()
		runtime.GC()

		errs := c.Receive()
		as.Errorf(<-errs, topic.ErrConsumerNotClosed, i)
	})
}
