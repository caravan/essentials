package retention

import "github.com/caravan/essentials/topic"

type (
	// CountedPolicy describes a Policy that only retains the most recent
	// specified Count of messages
	CountedPolicy interface {
		Policy
		Count() topic.Length
	}

	countedPolicy struct {
		count topic.Length
	}
)

// MakeCountedPolicy returns a Policy that only retains the most recent
// specified count of messages
func MakeCountedPolicy(c topic.Length) CountedPolicy {
	return &countedPolicy{
		count: c,
	}
}

func (p *countedPolicy) Count() topic.Length {
	return p.count
}

func (*countedPolicy) InitialState() State {
	return nil
}

func (p *countedPolicy) Retain(s State, r *Statistics) (State, bool) {
	return s, topic.Length(r.Entries.LastOffset) >= r.Log.Length-p.count
}
