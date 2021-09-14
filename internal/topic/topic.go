package topic

import (
	"sync"
	"time"

	"github.com/caravan/essentials/event"
	"github.com/caravan/essentials/id"
	"github.com/caravan/essentials/internal/sync/channel"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/essentials/topic/backoff"
	"github.com/caravan/essentials/topic/config"
	"github.com/caravan/essentials/topic/retention"
)

type (
	// Topic is the internal implementation of a Topic
	Topic struct {
		*config.Config
		retentionState retention.State
		log            *Log
		cursors        *cursors
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
func Make(o ...config.Option) *Topic {
	cfg := &config.Config{}
	withDefaults := append(o, config.Defaults)
	if err := config.ApplyOptions(cfg, withDefaults...); err != nil {
		panic(err)
	}

	res := &Topic{
		Config:         cfg,
		retentionState: cfg.RetentionPolicy.InitialState(),
		cursors:        makeCursors(),
		observers:      makeLogObservers(),
		log:            makeLog(cfg),
	}

	res.startVacuuming()
	return res
}

// Length returns the virtual size of the Topic
func (t *Topic) Length() topic.Length {
	return t.log.length()
}

// NewProducer instantiates a new Topic Producer
func (t *Topic) NewProducer() topic.Producer {
	return makeProducer(t)
}

// NewConsumer instantiates a new Topic Consumer
func (t *Topic) NewConsumer() topic.Consumer {
	return makeConsumer(t.makeCursor(), t.BackoffGenerator)
}

// Get consumes an event starting at the specified virtual Offset within
// the Topic. If the Offset is no longer being retained, the next
// available Offset will be consumed. The actual Offset read is returned
func (t *Topic) Get(o retention.Offset) (event.Event, retention.Offset, bool) {
	defer t.vacuumReady.Notify()
	e, o, ok := t.log.get(o)
	return e.event, o, ok
}

// Put adds the specified event to the Topic
func (t *Topic) Put(e event.Event) {
	t.log.put(e)
	t.notifyObservers()
}

func (t *Topic) isClosed() bool {
	return false
}

func (t *Topic) startVacuuming() {
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

func (t *Topic) vacuum() {
	baseStats := t.baseRetentionStatistics()
	t.log.vacuum(func(e *segment) bool {
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

func (t *Topic) baseRetentionStatistics() func() *retention.Statistics {
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

func (t *Topic) makeCursor() *cursor {
	c := makeCursor(t)
	t.cursors.track(c)
	t.observers.add(c.id, c.ready.Notify)
	return c
}

func (t *Topic) notifyObservers() {
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
