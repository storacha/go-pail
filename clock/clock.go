package clock

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"slices"
	"sync"

	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-pail/block"
	"github.com/storacha/go-pail/clock/event"
	"github.com/storacha/go-pail/ipld/node"
)

// Advance the clock by adding an event.
func Advance[T any](ctx context.Context, blocks block.Fetcher, dataBinder node.Binder[T], head []ipld.Link, evt ipld.Link) ([]ipld.Link, error) {
	events := event.NewFetcher(blocks, dataBinder)
	headmap := map[ipld.Link]struct{}{}
	for _, h := range head {
		headmap[h] = struct{}{}
	}
	if _, ok := headmap[evt]; ok {
		return head, nil
	}

	// does event contain the clock?
	var changed bool
	for _, h := range head {
		ok, err := contains(ctx, events, evt, h)
		if err != nil {
			return nil, err
		}
		if ok {
			delete(headmap, h)
			headmap[evt] = struct{}{}
			changed = true
		}
	}
	if changed {
		return slices.Collect(maps.Keys(headmap)), nil
	}

	// does clock contain the event?
	for _, h := range head {
		ok, err := contains(ctx, events, h, evt)
		if err != nil {
			return nil, err
		}
		if ok {
			return head, nil
		}
	}

	return append(head, evt), nil
}

// contains returns true if event "a" contains event "b". Breadth first search.
func contains[T any](ctx context.Context, events *event.Fetcher[T], a, b ipld.Link) (bool, error) {
	if a == b {
		return true, nil
	}

	var wg sync.WaitGroup
	wg.Add(2)

	var aevent event.Event[T]
	var bevent event.Event[T]
	var fetchErr error
	go func() {
		defer wg.Done()
		eb, err := events.Get(ctx, a)
		if err != nil {
			fetchErr = err
			return
		}
		aevent = eb.Value()
	}()
	go func() {
		defer wg.Done()
		eb, err := events.Get(ctx, b)
		if err != nil {
			fetchErr = err
			return
		}
		bevent = eb.Value()
	}()
	wg.Wait()
	if fetchErr != nil {
		return false, fetchErr
	}

	links := aevent.Parents()
	seen := map[ipld.Link]struct{}{}
	for len(links) > 0 {
		link := links[0]
		links = links[1:]
		if link == b {
			return true, nil
		}
		// if any of b's parents are this link, then b cannot exist in any of the
		// tree below, since that would create a cycle.
		if slices.Contains(bevent.Parents(), link) {
			continue
		}
		if _, ok := seen[link]; ok {
			continue
		}
		seen[link] = struct{}{}

		pbl, err := events.Get(ctx, link)
		if err != nil {
			return false, err
		}
		links = append(links, pbl.Value().Parents()...)
	}
	return false, nil
}

func Visualize[T any](ctx context.Context, blocks block.Fetcher, dataBinder node.Binder[T], head []ipld.Link) iter.Seq2[string, error] {
	events := event.NewFetcher(blocks, dataBinder)
	return func(yield func(string, error) bool) {
		if !yield("digraph clock {", nil) {
			return
		}
		if !yield("  node [shape=point fontname=Courier]; head;", nil) {
			return
		}

		var links []ipld.Link
		nodes := map[ipld.Link]struct{}{}
		for _, l := range head {
			e, err := events.Get(ctx, l)
			if err != nil {
				yield("", err)
				return
			}

			nodes[l] = struct{}{}
			if !yield(fmt.Sprintf(`  node [shape=oval fontname=Courier]; %s [label="%s"];`, l, shortLink(l)), nil) {
				return
			}
			if !yield(fmt.Sprintf(`  head -> %s;`, l), nil) {
				return
			}

			for _, p := range e.Value().Parents() {
				if !yield(fmt.Sprintf(`  %s -> %s;`, l, p), nil) {
					return
				}
			}

			links = append(links, e.Value().Parents()...)
		}

		for len(links) > 0 {
			l := links[0]
			links = links[1:]

			if _, ok := nodes[l]; ok {
				continue
			}
			nodes[l] = struct{}{}

			e, err := events.Get(ctx, l)
			if err != nil {
				yield("", err)
				return
			}

			if !yield(fmt.Sprintf(`  node [shape=oval]; %s [label="%s" fontname=Courier];`, l, shortLink(l)), nil) {
				return
			}

			for _, p := range e.Value().Parents() {
				if !yield(fmt.Sprintf(`  %s -> %s;`, l, p), nil) {
					return
				}
			}

			links = append(links, e.Value().Parents()...)
		}

		yield("}", nil)
	}
}

func shortLink(l ipld.Link) string {
	s := l.String()
	return fmt.Sprintf("%s..%s", s[:4], s[len(s)-4:])
}
