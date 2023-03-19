package topic

import (
	"sync"

	"github.com/caravan/essentials/closer"
	"github.com/caravan/essentials/id"
	"github.com/caravan/essentials/internal/sync/channel"
	"github.com/caravan/essentials/topic/retention"
)

type (
	// cursors manages a set of cursors on behalf of a Topic
	cursors[Msg any] struct {
		sync.RWMutex
		cursors map[id.ID]*cursor[Msg]
	}

	// cursor is used to consume log entries
	cursor[Msg any] struct {
		closer.Closer
		id     id.ID
		topic  *Topic[Msg]
		ready  *channel.ReadyWait
		offset retention.Offset
	}
)

func makeCursor[Msg any](t *Topic[Msg]) *cursor[Msg] {
	cID := id.New()
	ready := channel.MakeReadyWait()
	if t.Length() != 0 {
		ready.Notify()
	}

	return &cursor[Msg]{
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

func (c *cursor[Msg]) head() (Msg, bool) {
	if e, o, ok := c.topic.Get(c.offset); ok {
		c.offset = o
		return e, true
	}
	var zero Msg
	return zero, false
}

func (c *cursor[_]) advance() {
	c.offset = c.offset.Next()
}

func makeCursors[Msg any]() *cursors[Msg] {
	return &cursors[Msg]{
		cursors: map[id.ID]*cursor[Msg]{},
	}
}

func (c *cursors[Msg]) track(cursor *cursor[Msg]) {
	c.Lock()
	defer c.Unlock()
	i := cursor.id
	if _, ok := c.cursors[i]; !ok {
		c.cursors[i] = cursor
	}
}

func (c *cursors[_]) remove(i id.ID) {
	c.Lock()
	defer c.Unlock()
	delete(c.cursors, i)
}

func (c *cursors[_]) offsets() []retention.Offset {
	c.RLock()
	defer c.RUnlock()
	res := make([]retention.Offset, 0, len(c.cursors))
	for _, cursor := range c.cursors {
		res = append(res, cursor.offset)
	}
	return res
}
