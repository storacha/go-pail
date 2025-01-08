package crdt

import (
	"context"
	"errors"

	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-pail"
	"github.com/storacha/go-pail/block"
	"github.com/storacha/go-pail/clock"
	"github.com/storacha/go-pail/clock/event"
	"github.com/storacha/go-pail/crdt/operation"
	"github.com/storacha/go-pail/ipld/node"
	"github.com/storacha/go-pail/shard"
)

// Put a value (a CID) for the given key. If the key exists it's value is
// overwritten.
func Put(ctx context.Context, blocks block.Fetcher, head []ipld.Link, key string, value ipld.Link) (Result, error) {
	mblocks := block.NewMapBlockstore()
	blocks = block.NewTieredBlockFetcher(mblocks, blocks)

	if len(head) == 0 {
		rshard := shard.NewRoot(nil)

		rblock, err := shard.MarshalBlock(rshard)
		if err != nil {
			return Result{}, err
		}

		err = mblocks.Put(ctx, rblock)
		if err != nil {
			return Result{}, err
		}

		root, diff, err := pail.Put(ctx, blocks, rblock.Link(), key, value)
		if err != nil {
			return Result{}, err
		}

		data := operation.NewPut(root, key, value)
		eblock, err := event.MarshalBlock(event.NewEvent(data, head), node.UnbinderFunc[operation.Operation](operation.Unbind))
		if err != nil {
			return Result{}, err
		}

		head, err = clock.Advance(ctx, blocks, node.BinderFunc[operation.Operation](operation.Bind), head, eblock.Link())
		if err != nil {
			return Result{}, err
		}

		return Result{diff, root, head, eblock}, nil
	}

	return Result{}, errors.New("not implemented")
}
