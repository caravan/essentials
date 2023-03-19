package topic_test

import (
	"testing"
	"time"

	"github.com/caravan/essentials/topic"
	"github.com/caravan/essentials/topic/config"
	"github.com/caravan/essentials/topic/retention"
	"github.com/stretchr/testify/assert"

	_topic "github.com/caravan/essentials/internal/topic"
)

func TestMakeTopicError(t *testing.T) {
	as := assert.New(t)
	defer func() {
		as.Error(recover().(error))
	}()
	_topic.Make[any](config.Permanent, config.Consumed)
}

func TestLongLog(t *testing.T) {
	as := assert.New(t)

	l := _topic.Make[any](config.Permanent)
	for i := 0; i < 10000; i++ {
		l.Put(i)
	}
	as.Equal(topic.Length(10000), l.Length())

	for i := 0; i < 10000; i++ {
		e, o, ok := l.Get(retention.Offset(i))
		as.True(ok)
		as.Equal(i, e)
		as.Equal(retention.Offset(i), o)
	}
}

func TestUnknownOffset(t *testing.T) {
	as := assert.New(t)

	l := _topic.Make[any](config.Permanent)
	for i := 0; i < 100; i++ {
		l.Put(i)
	}

	e, _, ok := l.Get(retention.Offset(1000))
	as.Nil(e)
	as.False(ok)
}

func TestLogDiscarding(t *testing.T) {
	as := assert.New(t)

	segmentSize := config.DefaultSegmentIncrement
	l := _topic.Make[any](config.Consumed)
	for i := 0; i < segmentSize+3; i++ {
		l.Put(i)
	}

	time.Sleep(10 * time.Millisecond)
	e, o, ok := l.Get(retention.Offset(0))
	as.Equal(segmentSize, e)
	as.Equal(retention.Offset(segmentSize), o)
	as.True(ok)

}

func TestLogDiscardEverything(t *testing.T) {
	as := assert.New(t)

	segmentSize := config.DefaultSegmentIncrement
	l := _topic.Make[any](config.Consumed)
	for i := 0; i < segmentSize; i++ {
		l.Put(i)
	}

	time.Sleep(10 * time.Millisecond)
	e, o, ok := l.Get(retention.Offset(0))
	as.Nil(e)
	as.Equal(retention.Offset(segmentSize), o)
	as.False(ok)
}
