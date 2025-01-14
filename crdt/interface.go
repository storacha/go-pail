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
	// Event is the clock event block created for the operation performed.
	Event block.BlockView[event.Event[operation.Operation]]
}
