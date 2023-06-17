package retention

import (
	"time"

	"github.com/caravan/essentials/topic"
)

type (
	// Policy describes and implements a policy that a Topic will use to
	// discard messages
	Policy interface {
		InitialState() State
		Retain(State, *Statistics) (State, bool)
	}

	// State allows a Policy to accumulate state between Retain calls
	State any

	// Statistics provides just enough information about the Log and a
	// range entries to be useful to a Policy
	Statistics struct {
		CurrentTime time.Time
		Log         *LogStatistics
		Entries     *EntriesStatistics
	}

	// LogStatistics provides Retention information about the Log
	LogStatistics struct {
		Length        topic.Length
		CursorOffsets []Offset
	}

	// EntriesStatistics provides Retention information about a range of
	// Log entries
	EntriesStatistics struct {
		FirstOffset    Offset
		LastOffset     Offset
		FirstTimestamp time.Time
		LastTimestamp  time.Time
	}

	// Offset is a location within a Topic stream
	Offset uint64
)

// Next returns the next logical Offset. Should Offsets ever become something
// other than integers, this will spare consuming code
func (o Offset) Next() Offset {
	return o + 1
}
