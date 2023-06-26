package retention_test

import (
	"testing"
	"time"

	"github.com/caravan/essentials"
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

	for i := 0; i < segmentSize*4; i++ {
		p.Send() <- i
	}
	p.Close()

	for i := 0; i < segmentSize+11; i++ {
		as.Equal(i, <-c1.Receive())
	}

	time.Sleep(50 * time.Millisecond)
	c2 := top.NewConsumer()
	as.Equal(segmentSize, <-c2.Receive())

	c1.Close()
	c2.Close()
}
