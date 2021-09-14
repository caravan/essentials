package message_test

import (
	"testing"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/message"
	"github.com/stretchr/testify/assert"
)

func TestMustSend(t *testing.T) {
	as := assert.New(t)
	top := essentials.NewTopic()
	p := top.NewProducer()
	message.MustSend(p, "hello")
	p.Close()

	defer func() {
		as.Errorf(recover().(error), message.ErrSenderClosed)
	}()
	message.MustSend(p, "explode")
}
