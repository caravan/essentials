package retention_test

import (
	"testing"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/message"
	"github.com/caravan/essentials/topic/config"
	"github.com/caravan/essentials/topic/retention"
	"github.com/stretchr/testify/assert"
)

func TestPermanentPolicy(t *testing.T) {
	as := assert.New(t)
	p := retention.MakePermanentPolicy()
	as.NotNil(p)
}

func TestPermanent(t *testing.T) {
	as := assert.New(t)

	top := essentials.NewTopic[any](config.Permanent)
	p := top.NewProducer()

	for i := 0; i < 500; i++ {
		message.Send[any](p, i)
	}

	done := make(chan bool)
	go func() {
		c := top.NewConsumer()
		for i := 0; i < 500; i++ {
			as.Equal(i, message.MustReceive[any](c))
		}
		c.Close()
		done <- true
	}()

	<-done
	p.Close()
}
