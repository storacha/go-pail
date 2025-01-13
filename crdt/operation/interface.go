package operation

import "github.com/ipld/go-ipld-prime"

type Operation interface {
	// Root is the CID of the root shard of the pail after the operation was
	// performed.
	Root() ipld.Link
	// Type is the type of operation being performed "put" or "del".
	Type() string
	// Key is the key that is being operated on.
	Key() string
	// Value is the value to be put (nil if the operation is "del").
	Value() ipld.Link
}
