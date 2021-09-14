package debug

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/caravan/essentials/topic"
	"github.com/caravan/essentials/topic/config"
)

// ErrorWrapper is a function that wraps an error. It is returned by
// WrapStackTrace to attach stack information to a standard error
type ErrorWrapper func(error) error

// Environment variables
const (
	CaravanDebug = "CARAVAN_DEBUG"
)

// Error messages
const (
	MsgInstantiationTrace = "stack at time of instantiation"
)

var (
	trueMatcher = regexp.MustCompile("^\\s*(TRUE|YES|OK|1)\\s*$")
	makeTopic   func(o ...config.Option) topic.Topic

	debugSync    sync.Mutex
	debugEnabled bool
	debugTopic   topic.Topic
)

// ProvideDebugTopicMaker hands a constructor to the debugging interface
// that can be used to instantiate the debugTopic if needed
func ProvideDebugTopicMaker(m func(o ...config.Option) topic.Topic) {
	makeTopic = m
}

func needDebugTopic() {
	debugSync.Lock()
	defer debugSync.Unlock()
	if debugTopic == nil {
		debugTopic = makeTopic(config.Consumed)
	}
}

// Enable all debugging
func Enable() {
	debugSync.Lock()
	defer debugSync.Unlock()
	debugEnabled = true
}

// IsEnabled returns whether debugging is enabled
func IsEnabled() bool {
	debugSync.Lock()
	defer debugSync.Unlock()
	return debugEnabled
}

// WithProducer performs a callback, providing to it a debugging
// Producer whose lifecycle is managed by WithProducer itself
func WithProducer(with func(p topic.Producer)) {
	needDebugTopic()
	p := debugTopic.NewProducer()
	with(p)
	p.Close()
}

// WithConsumer performs a callback, providing to it a debugging
// Consumer whose lifecycle is managed by WithConsumer itself
func WithConsumer(with func(c topic.Consumer)) {
	needDebugTopic()
	c := debugTopic.NewConsumer()
	with(c)
	c.Close()
}

// TailLogTo will send debug Topic errors to the specified io.Writer.
// When CARAVAN_DEBUG is set, all reported errors are automatically tailed
// to os.Stderr
func TailLogTo(w io.Writer) {
	go func() {
		WithConsumer(func(c topic.Consumer) {
			for err := range c.Receive() {
				_, _ = fmt.Fprintf(w, "%s\n", err)
			}
		})
	}()
}

// WrapStackTrace returns an ErrorWrapper that attaches Stack information
// to an error based on the call stack when this function is invoked
func WrapStackTrace(msg string) ErrorWrapper {
	stack := string(debug.Stack())
	return func(e error) error {
		return fmt.Errorf("%w\n%s:\n%s", e, msg, stack)
	}
}

func init() {
	if s, ok := os.LookupEnv(CaravanDebug); ok {
		if trueMatcher.MatchString(strings.ToUpper(s)) {
			Enable()
			TailLogTo(os.Stderr)
		}
	}
}
