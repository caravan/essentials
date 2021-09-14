package topic

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/caravan/essentials/event"
	"github.com/caravan/essentials/internal/sync/mutex"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/essentials/topic/config"
	"github.com/caravan/essentials/topic/retention"
)

type (
	// Log manages a set of segments that contain Log entries
	Log struct {
		startOffset   uint64
		virtualLength uint64
		capIncrement  uint32
		head          headSegment
		tail          tailSegment
	}

	logEntry struct {
		event     event.Event
		createdAt time.Time
	}

	headSegment struct {
		sync.RWMutex
		segment *segment
	}

	tailSegment struct {
		sync.Mutex
		segment *segment
	}

	// segment manages a set of Log entries that includes Events and the
	// Time at which they were emitted
	segment struct {
		mutex.InitialMutex
		log     *Log
		next    *segment
		len     uint32
		cap     uint32
		entries []*logEntry
	}

	// retentionQuery is called by Log in order to determined if a segment
	// should be retained or discarded. Such a function is provided by a
	// Topic in order to apply a retention.Policy
	retentionQuery func(*segment) bool
)

var emptyLogEntry = &logEntry{}

func makeLog(cfg *config.Config) *Log {
	return &Log{
		capIncrement: uint32(cfg.SegmentIncrement),
	}
}

func (l *Log) start() retention.Offset {
	res := atomic.LoadUint64(&l.startOffset)
	return retention.Offset(res)
}

func (l *Log) length() topic.Length {
	res := atomic.LoadUint64(&l.virtualLength)
	return topic.Length(res)
}

func (l *Log) nextCapacity() uint32 {
	return l.capIncrement
}

func (l *Log) put(ev event.Event) {
	entry := &logEntry{
		event:     ev,
		createdAt: time.Now(),
	}

	l.tail.Lock()
	defer l.tail.Unlock()
	tail := l.tail.segment
	if tail == nil {
		l.head.Lock()
		defer l.head.Unlock()
		tail = l.makeSegment()
		l.head.segment = tail
		l.tail.segment = tail
	}
	if s := tail.append(entry); s != tail {
		l.tail.segment = s
	}
	atomic.AddUint64(&l.virtualLength, uint64(1))
}

func (l *Log) makeSegment() *segment {
	c := l.nextCapacity()
	return &segment{
		log:     l,
		cap:     c,
		entries: make([]*logEntry, c),
	}
}

func (l *Log) get(o retention.Offset) (*logEntry, retention.Offset, bool) {
	l.head.RLock()
	o, pos := l.relativePos(o)
	curr := l.head.segment
	l.head.RUnlock()

	for ; curr != nil && pos >= uint64(curr.cap); curr = curr.getNext() {
		pos -= uint64(curr.cap)
	}
	if curr != nil {
		p := int(pos)
		if p < int(curr.length()) {
			return curr.entries[p], o, true
		}
	}
	return emptyLogEntry, o, false
}

func (l *Log) relativePos(o retention.Offset) (retention.Offset, uint64) {
	eo := retention.Offset(l.startOffset)
	if o < eo { // if requested is less than actual, we start at actual
		o = eo
	}
	return o, uint64(o - eo)
}

func (l *Log) canVacuum() bool {
	l.head.RLock()
	defer l.head.RUnlock()
	if head := l.head.segment; head != nil {
		return !head.isActive()
	}
	return false
}

func (l *Log) vacuum(retain retentionQuery) {
	l.head.Lock()
	defer l.head.Unlock()

	for curr := l.head.segment; curr != nil; {
		if curr.isActive() || retain(curr) {
			return // stop as soon as we see an active or retained segment
		}
		l.startOffset += uint64(curr.cap)
		if curr = curr.getNext(); curr != nil {
			l.head.segment = curr
			continue
		}
		l.tail.Lock()
		l.head.segment = nil
		l.tail.segment = nil
		l.tail.Unlock()
		return
	}
}

func (s *segment) getNext() *segment {
	s.Lock()
	defer s.Unlock()
	return s.next
}

func (s *segment) append(entry *logEntry) *segment {
	s.Lock()
	defer s.Unlock()
	if s.len == s.cap {
		s.next = s.log.makeSegment()
		s.DisableLock()
		return s.next.append(entry)
	}
	s.entries[s.len] = entry
	atomic.AddUint32(&s.len, uint32(1))
	return s
}

func (s *segment) length() uint32 {
	return atomic.LoadUint32(&s.len)
}

func (s *segment) isActive() bool {
	return !s.isFull()
}

func (s *segment) isFull() bool {
	return s.length() == s.cap
}

func (s *segment) timeRange() (time.Time, time.Time) {
	f := s.entries[0].createdAt
	l := s.entries[s.len-1].createdAt
	return f, l
}
