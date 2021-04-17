package essentials

import (
	"github.com/caravan/essentials/topic"
	"github.com/caravan/essentials/topic/config"

	"github.com/caravan/essentials/internal/debug"
	_topic "github.com/caravan/essentials/internal/topic"
)

// NewTopic instantiates a new Topic, given the specified Options
func NewTopic(o ...config.Option) topic.Topic {
	return _topic.Make(o...)
}

func init() {
	debug.ProvideDebugTopicMaker(NewTopic)
}
