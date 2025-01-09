package crdt

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-pail"
	"github.com/storacha/go-pail/block"
	"github.com/storacha/go-pail/clock"
	"github.com/storacha/go-pail/clock/event"
	"github.com/storacha/go-pail/crdt/operation"
	"github.com/storacha/go-pail/ipld/node"
	"github.com/storacha/go-pail/shard"
)

var ErrEventNotFound = errors.New("event not found")

// Put a value (a CID) for the given key. If the key exists it's value is
// overwritten.
func Put(ctx context.Context, blocks block.Fetcher, head []ipld.Link, key string, value ipld.Link) (Result, error) {
	mblocks := block.NewMapBlockstore()
	blocks = block.NewTieredBlockFetcher(mblocks, blocks)

	if len(head) == 0 {
		rshard := shard.NewRoot(nil)

		rblock, err := shard.MarshalBlock(rshard)
		if err != nil {
			return Result{}, fmt.Errorf("marshalling shard: %w", err)
		}

		err = mblocks.Put(ctx, rblock)
		if err != nil {
			return Result{}, fmt.Errorf("putting root block: %w", err)
		}

		root, diff, err := pail.Put(ctx, blocks, rblock.Link(), key, value)
		if err != nil {
			return Result{}, fmt.Errorf("putting value for key: %w", err)
		}

		data := operation.NewPut(root, key, value)
		eblock, err := event.MarshalBlock(event.NewEvent(data, head), node.UnbinderFunc[operation.Operation](operation.Unbind))
		if err != nil {
			return Result{}, fmt.Errorf("marshalling event: %w", err)
		}

		head, err = clock.Advance(ctx, blocks, node.BinderFunc[operation.Operation](operation.Bind), head, eblock.Link())
		if err != nil {
			return Result{}, fmt.Errorf("advancing clock: %w", err)
		}

		return Result{diff, root, head, eblock}, nil
	}

	return Result{}, errors.New("not implemented")
}

// Root determines the effective pail root given the current merkle clock head.
//
// Clocks with multiple head events may return blocks that were added or removed
// while playing forward events from their common ancestor.
func Root(ctx context.Context, blocks block.Fetcher, head []ipld.Link) (ipld.Link, shard.Diff, error) {
	if len(head) == 0 {
		return nil, shard.Diff{}, errors.New("cannot determine root of headless clock")
	}

	mblocks := block.NewMapBlockstore()
	blocks = block.NewTieredBlockFetcher(mblocks, blocks)
	events := event.NewFetcher(blocks, node.BinderFunc[operation.Operation](operation.Bind))

	if len(head) == 1 {
		event, err := events.Get(ctx, head[0])
		if err != nil {
			return nil, shard.Diff{}, fmt.Errorf("getting head event: %w", err)
		}
		return event.Value().Data().Root(), shard.Diff{}, nil
	}

	ancestor, err := findCommonAncestor(ctx, events, head)
	if err != nil {
		return nil, shard.Diff{}, fmt.Errorf("finding common ancestor event: %w", err)
	}

	aevent, err := events.Get(ctx, ancestor)
	if err != nil {
		return nil, shard.Diff{}, fmt.Errorf("getting ancestor event: %w", err)
	}

	return nil, shard.Diff{}, errors.New("not implemented")
}

// findCommonAncestor finds the common ancestor event of the passed children. A
// common ancestor is the first single event in the DAG that _all_ paths from
// children lead to.
func findCommonAncestor(ctx context.Context, events *event.Fetcher[operation.Operation], children []ipld.Link) (ipld.Link, error) {
	if len(children) == 0 {
		return nil, ErrEventNotFound
	}

	candidates := [][]ipld.Link{}
	for _, ch := range children {
		candidates = append(candidates, []ipld.Link{ch})
	}
	for {
		var changed bool
		for _, c := range candidates {
			candidate, err := findAncestorCandidate(ctx, events, c[len(c)-1])
			if err != nil {
				if errors.Is(err, ErrEventNotFound) {
					continue
				}
				return nil, err
			}

			changed = true
			c = append(c, candidate)

			if ancestor := findCommonLink(candidates); ancestor != nil {
				return ancestor, nil
			}
		}
		if !changed {
			return nil, ErrEventNotFound
		}
	}
}

func findAncestorCandidate(ctx context.Context, events *event.Fetcher[operation.Operation], root ipld.Link) (ipld.Link, error) {
	eblock, err := events.Get(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("getting event: %w", err)
	}

	parents := eblock.Value().Parents()
	if len(parents) == 0 {
		return root, nil
	}

	if len(parents) == 1 {
		return parents[0], nil
	}

	return findCommonAncestor(ctx, events, parents)
}

func findCommonLink(arrays [][]ipld.Link) ipld.Link {
	for i, arr := range arrays {
		for _, link := range arr {
			matched := true
			for j, other := range arrays {
				if i == j {
					continue
				}
				matched = slices.Contains(other, link)
				if !matched {
					break
				}
			}
			if matched {
				return link
			}
		}
	}
	return nil
}
