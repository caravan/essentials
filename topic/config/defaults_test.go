package config_test

import (
	"testing"

	caravan "github.com/caravan/essentials"
	"github.com/caravan/essentials/topic/config"
	"github.com/stretchr/testify/assert"
)

func TestDefaults(t *testing.T) {
	as := assert.New(t)
	top1 := caravan.NewTopic(config.Defaults, config.Defaults)
	as.NotNil(top1)

	top2 := caravan.NewTopic(config.Permanent, config.Defaults)
	as.NotNil(top2)

	top3 := caravan.NewTopic(config.Consumed, config.Defaults)
	as.NotNil(top3)
}
