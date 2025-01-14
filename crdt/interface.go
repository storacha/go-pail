package crdt

import (
	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-pail/block"
	"github.com/storacha/go-pail/clock/event"
	"github.com/storacha/go-pail/crdt/operation"
	"github.com/storacha/go-pail/shard"
)

type Result struct {
	shard.Diff
	// Root is the new CID of the root shard of the pail.
	Root ipld.Link
	// Head is the list of event CIDs at the head of the clock.
	Head []ipld.Link
	// Event is the clock event block created for the operation performed. This
	// may be nil of no operation occurred, for example putting the same value to
	// an existing key or deleting a key that does not exist.
	Event block.BlockView[event.Event[operation.Operation]]
}
