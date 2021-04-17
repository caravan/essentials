package config_test

import (
	"testing"
	"time"

	caravan "github.com/caravan/essentials"
	"github.com/caravan/essentials/topic/backoff"
	"github.com/caravan/essentials/topic/config"
	"github.com/stretchr/testify/assert"
)

const errBackoffExplosion = "intentional backoff explosion"

func TestBackoffConflict(t *testing.T) {
	as := assert.New(t)

	defer func() {
		rec := recover()
		as.NotNil(rec)
		as.Errorf(rec.(error), config.ErrBackoffAlreadySet)
	}()

	caravan.NewTopic(
		config.FixedBackoffSequence(10),
		config.FibonacciBackoffSequence(time.Microsecond, 100),
	)
}

func TestBackoffGeneratorOption(t *testing.T) {
	as := assert.New(t)
	defer func() {
		as.Equal(errBackoffExplosion, recover())
	}()

	caravan.NewTopic(
		config.BackoffGenerator(func() backoff.Next {
			panic(errBackoffExplosion)
		}),
	).NewConsumer()
}
