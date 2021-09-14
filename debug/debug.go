package debug

import "github.com/caravan/essentials/internal/debug"

var (
	// Enable enables debugging information to be emitted
	Enable = debug.Enable

	// IsEnabled returns whether debugging information is enabled
	IsEnabled = debug.IsEnabled

	// WithConsumer provides a debugging Consumer to the provided callback
	// function. That Consumer is managed by the call to WithConsumer, so
	// it need not be closed explicitly by the caller
	WithConsumer = debug.WithConsumer

	// TailLogTo begins to stream any new debugging information to the
	// specified io.Writer. Debugging information must be enabled for this
	// call produce any output
	TailLogTo = debug.TailLogTo
)
