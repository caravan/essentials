package id_test

import (
	"testing"

	"github.com/caravan/essentials/id"
	"github.com/stretchr/testify/assert"
)

func TestID(t *testing.T) {
	as := assert.New(t)

	i1 := id.New()
	i2 := id.Nil
	as.NotEqual(i1, i2)
	as.NotEqual(i1.String(), i2.String())
	as.Equal("00000000-0000-0000-0000-000000000000", i2.String())
}
