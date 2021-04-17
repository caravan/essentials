package channel_test

import (
	"testing"
	"time"

	"github.com/caravan/essentials/internal/sync/channel"
)

func TestTimeout(t *testing.T) {
	early := make(chan bool)
	go func() {
		time.Sleep(500 * time.Millisecond)
		early <- true
	}()
	select {
	case <-channel.Timeout(50 * time.Millisecond):
		// This is good
	case <-early:
		t.Errorf("Timeout should have happened first")
	}
}
