package topic

import (
	"sync"
	"time"

	"github.com/caravan/essentials/id"
	"github.com/caravan/essentials/internal/sync/channel"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/essentials/topic/backoff"
	"github.com/caravan/essentials/topic/config"
	"github.com/caravan/essentials/topic/retention"
)

type (
	// Topic is the internal implementation of a Topic
	Topic[Msg any] struct {
		*config.Config
		retentionState retention.State
		log            *Log[Msg]
		cursors        *cursors[Msg]
		observers      *topicObservers
		vacuumReady    *channel.ReadyWait
	}

	// topicObservers manages a set of callbacks for observers of a Topic
	topicObservers struct {
		sync.RWMutex
		callbacks map[id.ID]func()
	}
)

// Make instantiates a new internal Topic instance
func Make[Msg any](o ...config.Option) *Topic[Msg] {
	cfg := &config.Config{}
	withDefaults := append(o, config.Defaults)
	if err := config.ApplyOptions(cfg, withDefaults...); err != nil {
		panic(err)
	}

	res := &Topic[Msg]{
		Config:         cfg,
		retentionState: cfg.RetentionPolicy.InitialState(),
		cursors:        makeCursors[Msg](),
		observers:      makeLogObservers(),
		log:            makeLog[Msg](cfg),
	}

	res.startVacuuming()
	return res
}

// Length returns the virtual size of the Topic
func (t *Topic[_]) Length() topic.Length {
	return t.log.length()
}

// NewProducer instantiates a new Topic Producer
func (t *Topic[Msg]) NewProducer() topic.Producer[Msg] {
	return makeProducer(t)
}

// NewConsumer instantiates a new Topic Consumer
func (t *Topic[Msg]) NewConsumer() topic.Consumer[Msg] {
	return makeConsumer(t.makeCursor(), t.BackoffGenerator)
}

// Get consumes an message starting at the specified virtual Offset within the
// Topic. If the Offset is no longer being retained, the next available Offset
// will be consumed. The actual Offset read is returned
func (t *Topic[Msg]) Get(o retention.Offset) (Msg, retention.Offset, bool) {
	defer t.vacuumReady.Notify()
	e, o, ok := t.log.get(o)
	return e.msg, o, ok
}

// Put adds the specified Message to the Topic
func (t *Topic[Msg]) Put(msg Msg) {
	t.log.put(msg)
	t.notifyObservers()
}

func (t *Topic[_]) isClosed() bool {
	return false
}

func (t *Topic[_]) startVacuuming() {
	vacuumID := id.New()
	ready := channel.MakeReadyWait()
	t.vacuumReady = ready
	t.observers.add(vacuumID, ready.Notify)

	go func() {
		b := backoff.DefaultGenerator
		next := b()
		for !t.isClosed() {
			select {
			case <-channel.Timeout(next()):
			case <-ready.Wait():
			}
			if t.log.canVacuum() {
				t.vacuum()
				next = b()
			}
		}
	}()
}

func (t *Topic[Msg]) vacuum() {
	baseStats := t.baseRetentionStatistics()
	t.log.vacuum(func(e *segment[Msg]) bool {
		start := t.log.start()
		firstTimestamp, lastTimestamp := e.timeRange()
		stats := *baseStats()
		stats.Entries = &retention.EntriesStatistics{
			FirstOffset:    start,
			LastOffset:     start + retention.Offset(e.length()-1),
			FirstTimestamp: firstTimestamp,
			LastTimestamp:  lastTimestamp,
		}
		s, r := t.RetentionPolicy.Retain(t.retentionState, &stats)
		t.retentionState = s
		return r
	})
}

func (t *Topic[_]) baseRetentionStatistics() func() *retention.Statistics {
	var base *retention.Statistics
	return func() *retention.Statistics {
		if base == nil {
			base = &retention.Statistics{
				CurrentTime: time.Now(),
				Log: &retention.LogStatistics{
					Length:        t.log.length(),
					CursorOffsets: t.cursors.offsets(),
				},
			}
		}
		return base
	}
}

func (t *Topic[Msg]) makeCursor() *cursor[Msg] {
	c := makeCursor(t)
	t.cursors.track(c)
	t.observers.add(c.id, c.ready.Notify)
	return c
}

func (t *Topic[_]) notifyObservers() {
	t.observers.notify()
}

func makeLogObservers() *topicObservers {
	return &topicObservers{
		callbacks: map[id.ID]func(){},
	}
}

func (o *topicObservers) add(i id.ID, cb func()) {
	o.Lock()
	defer o.Unlock()
	o.callbacks[i] = cb
}

func (o *topicObservers) remove(i id.ID) {
	o.Lock()
	defer o.Unlock()
	delete(o.callbacks, i)
}

func (o *topicObservers) notify() {
	o.RLock()
	defer o.RUnlock()
	for _, cb := range o.callbacks {
		cb()
	}
}
