package config_test

import (
	"testing"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/topic/config"
	"github.com/stretchr/testify/assert"
)

func TestDefaults(t *testing.T) {
	as := assert.New(t)
	top1 := essentials.NewTopic(config.Defaults, config.Defaults)
	as.NotNil(top1)

	top2 := essentials.NewTopic(config.Permanent, config.Defaults)
	as.NotNil(top2)

	top3 := essentials.NewTopic(config.Consumed, config.Defaults)
	as.NotNil(top3)
}
