package retention_test

import (
	"testing"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/essentials/topic/config"
	"github.com/caravan/essentials/topic/retention"
	"github.com/stretchr/testify/assert"
)

func TestConsumedPolicy(t *testing.T) {
	as := assert.New(t)
	p := retention.MakeConsumedPolicy()
	as.NotNil(p)
}

func TestMakeConsumedSome(t *testing.T) {
	as := assert.New(t)
	top := essentials.NewTopic(config.Consumed)

	segmentSize := config.DefaultSegmentIncrement
	p := top.NewProducer()
	c1 := top.NewConsumer()

	for i := 0; i < segmentSize*4; i++ {
		p.Send(i)
	}
	as.Nil(p.Close())

	for i := 0; i < segmentSize+11; i++ {
		as.Equal(i, topic.MustReceive(c1))
	}

	c2 := top.NewConsumer()
	as.Equal(segmentSize, topic.MustReceive(c2))

	as.Nil(c1.Close())
	as.Nil(c2.Close())
}
