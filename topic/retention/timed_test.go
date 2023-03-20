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

func TestTimedPolicy(t *testing.T) {
	as := assert.New(t)
	p := retention.MakeTimedPolicy(144 * time.Millisecond)
	as.NotNil(p)
	as.Equal(time.Millisecond*144, p.Duration())
}

func TestTimed(t *testing.T) {
	as := assert.New(t)
	top := essentials.NewTopic[any](config.Timed(50 * time.Millisecond))
	segmentSize := config.DefaultSegmentIncrement
	p := top.NewProducer()
	c := top.NewConsumer()
	msg := message.Of[any]()

	for i := 0; i < segmentSize; i++ {
		msg.Send(p, i)
	}

	time.Sleep(100 * time.Millisecond)

	for i := segmentSize; i < segmentSize*2; i++ {
		msg.Send(p, i)
	}

	as.Equal(segmentSize, msg.MustReceive(c))
	p.Close()
	c.Close()
}
