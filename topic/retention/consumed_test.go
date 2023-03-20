package retention_test

import (
	"testing"
	"time"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/message"
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
	top := essentials.NewTopic[any](config.Consumed)

	segmentSize := config.DefaultSegmentIncrement
	p := top.NewProducer()
	c1 := top.NewConsumer()
	msg := message.Of[any]()

	for i := 0; i < segmentSize*4; i++ {
		msg.Send(p, i)
	}
	p.Close()

	for i := 0; i < segmentSize+11; i++ {
		as.Equal(i, msg.MustReceive(c1))
	}

	time.Sleep(50 * time.Millisecond)
	c2 := top.NewConsumer()
	as.Equal(segmentSize, msg.MustReceive(c2))

	c1.Close()
	c2.Close()
}
