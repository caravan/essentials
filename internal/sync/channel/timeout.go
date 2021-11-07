package channel

import "time"

// Timeout returns a receive-only channel that will be closed after the
// specified Duration. The value of a structure like this over a direct call to
// time.Sleep is that a channel can participate in a select
func Timeout(d time.Duration) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		time.Sleep(d)
		close(ch)
	}()
	return ch
}
