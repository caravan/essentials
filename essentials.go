package essentials

import (
	"github.com/caravan/essentials/topic"
	"github.com/caravan/essentials/topic/config"

	internal "github.com/caravan/essentials/internal/topic"
)

// NewTopic instantiates a new Topic, given the specified Options
func NewTopic[Msg any](o ...config.Option) topic.Topic[Msg] {
	return internal.Make[Msg](o...)
}
