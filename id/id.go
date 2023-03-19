package id

import "github.com/google/uuid"

// ID is a unique identifier
type ID uuid.UUID

// Nil is an empty (zero) ID
var Nil = ID(uuid.Nil)

// New returns a new unique ID
func New() ID {
	return ID(uuid.New())
}

func (id ID) String() string {
	return uuid.UUID(id).String()
}

// Bytes allows ID to be used as a Key
func (id ID) Bytes() []byte {
	return id[:]
}

// Equal allows ID to be compared for equality
func (id ID) Equal(other any) bool {
	oid, ok := other.(ID)
	return ok && id == oid
}
