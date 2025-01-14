package pail

import (
	"fmt"

	"github.com/storacha/go-pail/block"
	"github.com/storacha/go-pail/shard"
)

// New creates a new empty pail. It encodes and hashes the data and returns a
// block view of the root shard.
func New() (block.BlockView[shard.RootShard], error) {
	rs := shard.NewRoot(nil)
	rb, err := shard.MarshalBlock(rs)
	if err != nil {
		return nil, fmt.Errorf("marshalling pail root: %w", err)
	}
	return rb, nil
}
