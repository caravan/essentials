package retention_test

import (
	"testing"

	caravan "github.com/caravan/essentials"
	"github.com/caravan/essentials/topic"
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

	top := caravan.NewTopic(config.Permanent)
	p := top.NewProducer()

	for i := 0; i < 500; i++ {
		p.Send(i)
	}

	done := make(chan bool)
	go func() {
		c := top.NewConsumer()
		for i := 0; i < 500; i++ {
			as.Equal(i, topic.MustReceive(c))
		}
		as.Nil(c.Close())
		done <- true
	}()

	<-done
	as.Nil(p.Close())
}
