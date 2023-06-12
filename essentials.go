package essentials

import (
	"github.com/caravan/essentials/internal/debug"
	"github.com/caravan/essentials/topic"
	"github.com/caravan/essentials/topic/config"
)

// NewTopic instantiates a new Topic, given the specified Options
func NewTopic[Msg any](o ...config.Option) topic.Topic[Msg] {
	return Of[Msg]().NewTopic(o...)
}

func init() {
	debug.ProvideDebugTopicMaker(NewTopic[error])
}
