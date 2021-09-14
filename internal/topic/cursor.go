package topic

import (
	"sync"

	"github.com/caravan/essentials/closer"
	"github.com/caravan/essentials/id"
	"github.com/caravan/essentials/internal/sync/channel"
	"github.com/caravan/essentials/message"
	"github.com/caravan/essentials/topic/retention"
)

type (
	// cursors manages a set of cursors on behalf of a Topic
	cursors struct {
		sync.RWMutex
		cursors map[id.ID]*cursor
	}

	// cursor is used to consume log entries
	cursor struct {
		closer.Closer
		id     id.ID
		topic  *Topic
		ready  *channel.ReadyWait
		offset retention.Offset
	}
)

func makeCursor(t *Topic) *cursor {
	cID := id.New()
	ready := channel.MakeReadyWait()
	if t.Length() != 0 {
		ready.Notify()
	}

	return &cursor{
		id:    cID,
		topic: t,
		ready: ready,
		Closer: makeCloser(func() {
			ready.Close()
			t.cursors.remove(cID)
			t.observers.remove(cID)
		}),
	}
}

func (c *cursor) head() (message.Event, bool) {
	if e, o, ok := c.topic.Get(c.offset); ok {
		c.offset = o
		return e, true
	}
	return nil, false
}

func (c *cursor) advance() {
	c.offset = c.offset.Next()
}

func makeCursors() *cursors {
	return &cursors{
		cursors: map[id.ID]*cursor{},
	}
}

func (c *cursors) track(cursor *cursor) {
	c.Lock()
	defer c.Unlock()
	i := cursor.id
	if _, ok := c.cursors[i]; !ok {
		c.cursors[i] = cursor
	}
}

func (c *cursors) remove(i id.ID) {
	c.Lock()
	defer c.Unlock()
	delete(c.cursors, i)
}

func (c *cursors) offsets() []retention.Offset {
	c.RLock()
	defer c.RUnlock()
	res := make([]retention.Offset, 0, len(c.cursors))
	for _, cursor := range c.cursors {
		res = append(res, cursor.offset)
	}
	return res
}
