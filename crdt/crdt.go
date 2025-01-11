package crdt

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
	"sync"

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
	root := aevent.Value().Data().Root()

	sorted, err := findSortedEvents(ctx, events, head, ancestor)
	if err != nil {
		return nil, shard.Diff{}, fmt.Errorf("finding sorted events: %w", err)
	}

	additions := map[ipld.Link]shard.BlockView{}
	removals := map[ipld.Link]shard.BlockView{}

	for _, eblock := range sorted {
		event := eblock.Value()

		var diff shard.Diff
		var err error
		if event.Data().Type() == operation.TypePut {
			root, diff, err = pail.Put(ctx, blocks, root, event.Data().Key(), event.Data().Value())
			if err != nil {
				return nil, shard.Diff{}, fmt.Errorf("putting to common ancestor: %w", err)
			}
		} else if event.Data().Type() == operation.TypeDel {
			root, diff, err = pail.Del(ctx, blocks, root, event.Data().Key())
			if err != nil {
				return nil, shard.Diff{}, fmt.Errorf("deleting from common ancestor: %w", err)
			}
		} else {
			return nil, shard.Diff{}, fmt.Errorf("unknown operation: %s", event.Data().Type())
		}

		for _, a := range diff.Additions {
			mblocks.Put(ctx, a)
			additions[a.Link()] = a
		}
		for _, r := range diff.Removals {
			removals[r.Link()] = r
		}
	}

	// filter blocks that were added _and_ removed
	for k := range maps.Keys(removals) {
		if _, ok := additions[k]; ok {
			delete(additions, k)
			delete(removals, k)
		}
	}

	diff := shard.Diff{
		Additions: slices.Collect(maps.Values(additions)),
		Removals:  slices.Collect(maps.Values(removals)),
	}

	return root, diff, nil
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
		for i, c := range candidates {
			candidate, err := findAncestorCandidate(ctx, events, c[len(c)-1])
			if err != nil {
				if errors.Is(err, ErrEventNotFound) {
					continue
				}
				return nil, err
			}

			changed = true
			candidates[i] = append(c, candidate)

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

type weightedEvent struct {
	event  event.BlockView[operation.Operation]
	weight int64
}

// findSortedEvents finds and sorts events between the head(s) and the tail.
func findSortedEvents(ctx context.Context, events *event.Fetcher[operation.Operation], head []ipld.Link, tail ipld.Link) ([]event.BlockView[operation.Operation], error) {
	if len(head) == 1 && head[0] == tail {
		return []event.BlockView[operation.Operation]{}, nil
	}

	// get weighted events - heavier events happened first
	weights := map[ipld.Link]weightedEvent{}
	for arr, err := range findAllEvents(ctx, events, head, tail, 0) {
		if err != nil {
			return nil, fmt.Errorf("finding events: %w", err)
		}
		for _, we := range arr {
			if info, ok := weights[we.event.Link()]; ok {
				info.weight += we.weight
			} else {
				weights[we.event.Link()] = we
			}
		}
	}

	// group events into buckets by weight
	buckets := map[int64][]event.BlockView[operation.Operation]{}
	for _, we := range weights {
		if bucket, ok := buckets[we.weight]; ok {
			buckets[we.weight] = append(bucket, we.event)
		} else {
			buckets[we.weight] = []event.BlockView[operation.Operation]{we.event}
		}
	}

	// sort by weight, and by CID within weight
	sortedWeights := slices.Collect(maps.Keys(buckets))
	slices.Sort(sortedWeights)
	var sortedEvents []event.BlockView[operation.Operation]
	for _, w := range sortedWeights {
		wes := buckets[w]
		slices.SortFunc(wes, func(a, b event.BlockView[operation.Operation]) int {
			if a.Link().String() < b.Link().String() {
				return -1
			}
			return 1
		})
		sortedEvents = append(sortedEvents, wes...)
	}

	return sortedEvents, nil
}

func findAllEvents(ctx context.Context, events *event.Fetcher[operation.Operation], head []ipld.Link, tail ipld.Link, depth int64) iter.Seq2[[]weightedEvent, error] {
	return func(yield func([]weightedEvent, error) bool) {
		var wg sync.WaitGroup
		wg.Add(len(head))

		var mutex sync.RWMutex
		var stopped bool
		cctx, cancel := context.WithCancel(ctx)
		for _, h := range head {
			go func() {
				defer wg.Done()
				var wevts []weightedEvent
				for we, err := range findEvents(cctx, events, h, tail, depth+1) {
					mutex.Lock()
					if stopped {
						mutex.Unlock()
						return
					}
					if err != nil {
						stopped = true
						cancel() // cancel other fetches
						mutex.Unlock()
						return
					}
					mutex.Unlock()
					wevts = append(wevts, we)
				}

				mutex.Lock()
				if stopped {
					mutex.Unlock()
					return
				}
				if !yield(wevts, nil) {
					stopped = true
					mutex.Unlock()
					return
				}
				mutex.Unlock()
			}()
		}
		wg.Wait()
		cancel()
	}
}

func findEvents(ctx context.Context, events *event.Fetcher[operation.Operation], head ipld.Link, tail ipld.Link, depth int64) iter.Seq2[weightedEvent, error] {
	return func(yield func(weightedEvent, error) bool) {
		event, err := events.Get(ctx, head)
		if err != nil {
			yield(weightedEvent{}, err)
			return
		}

		if !yield(weightedEvent{event, depth}, nil) {
			return
		}

		parents := event.Value().Parents()
		if len(parents) == 1 && parents[0] == tail {
			return
		}

		var wg sync.WaitGroup
		wg.Add(len(parents))

		var mutex sync.RWMutex
		var stopped bool
		cctx, cancel := context.WithCancel(ctx)
		for _, p := range parents {
			go func() {
				defer wg.Done()
				for we, err := range findEvents(cctx, events, p, tail, depth+1) {
					mutex.Lock()
					if stopped {
						mutex.Unlock()
						return
					}
					if err != nil {
						stopped = true
						yield(weightedEvent{}, err)
						cancel() // cancel other fetches
						mutex.Unlock()
						return
					}
					if !yield(we, err) {
						stopped = true
						mutex.Unlock()
						return
					}
					mutex.Unlock()
				}
			}()
		}
		wg.Wait()
		cancel()
	}
}
