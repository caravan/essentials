package retention

import (
	"time"

	"github.com/caravan/essentials/topic"
)

type (
	// Policy describes and implements a policy that a Topic will use to
	// discard Events
	Policy interface {
		InitialState() State
		Retain(State, *Statistics) (State, bool)
	}

	// State allows a Policy to accumulate state between Retain calls
	State interface{}

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
		CursorOffsets []topic.Offset
	}

	// EntriesStatistics provides Retention information about a range of
	// Log entries
	EntriesStatistics struct {
		FirstOffset    topic.Offset
		LastOffset     topic.Offset
		FirstTimestamp time.Time
		LastTimestamp  time.Time
	}
)
