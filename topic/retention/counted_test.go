package retention_test

import (
	"testing"
	"time"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/message"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/essentials/topic/config"
	"github.com/caravan/essentials/topic/retention"
	"github.com/stretchr/testify/assert"
)

func TestCountedPolicy(t *testing.T) {
	as := assert.New(t)
	p := retention.MakeCountedPolicy(100)
	as.NotNil(p)
	as.Equal(topic.Length(100), p.Count())
}

func TestCounted(t *testing.T) {
	as := assert.New(t)
	top := essentials.NewTopic(config.Counted(100))

	p := top.NewProducer()
	for i := 0; i < 256; i++ {
		message.Send(p, i)
	}

	time.Sleep(100 * time.Millisecond)
	c := top.NewConsumer()
	as.Equal(128, message.MustReceive(c))

	p.Close()
	c.Close()
}
