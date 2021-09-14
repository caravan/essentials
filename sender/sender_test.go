package sender_test

import (
	"testing"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/sender"
	"github.com/stretchr/testify/assert"
)

func TestMustSend(t *testing.T) {
	as := assert.New(t)
	top := essentials.NewTopic()
	p := top.NewProducer()
	sender.MustSend(p, "hello")
	p.Close()

	defer func() {
		as.Errorf(recover().(error), sender.ErrClosed)
	}()
	sender.MustSend(p, "explode")
}
