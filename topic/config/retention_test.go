package config_test

import (
	"testing"

	"github.com/caravan/essentials"
	"github.com/caravan/essentials/topic/config"
	"github.com/caravan/essentials/topic/retention"
	"github.com/stretchr/testify/assert"
)

type explodingRetentionPolicy bool

const errRetentionExplosion = "intentional retention explosion"

func TestRetentionConflict(t *testing.T) {
	as := assert.New(t)

	defer func() {
		rec := recover()
		as.NotNil(rec)
		as.Errorf(rec.(error), config.ErrRetentionPolicyAlreadySet)
	}()

	essentials.NewTopic(config.Timed(5), config.Counted(10))
}

func TestRetentionPolicyOption(t *testing.T) {
	as := assert.New(t)
	defer func() {
		as.Equal(errRetentionExplosion, recover())
	}()

	essentials.NewTopic(
		config.RetentionPolicy(explodingRetentionPolicy(false)),
	)
}

func (explodingRetentionPolicy) InitialState() retention.State {
	panic(errRetentionExplosion)
}

func (explodingRetentionPolicy) Retain(_ retention.State, _ *retention.Statistics) (retention.State, bool) {
	panic(errRetentionExplosion)
}
