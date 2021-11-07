package retention

type (
	// PermanentPolicy describes a Policy where all Events are retained
	// without consideration.
	PermanentPolicy interface {
		Policy
	}

	permanentPolicy struct{}
)

var _permanentPolicy = &permanentPolicy{}

// MakePermanentPolicy returns a Policy where all Events are retained without
// consideration
func MakePermanentPolicy() PermanentPolicy {
	return _permanentPolicy
}

func (*permanentPolicy) InitialState() State {
	return nil
}

func (*permanentPolicy) Retain(s State, _ *Statistics) (State, bool) {
	return s, true
}
