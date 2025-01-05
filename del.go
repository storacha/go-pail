package pail

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-pail/block"
	"github.com/storacha/go-pail/shard"
)

// Del deletes the value for the given key from the bucket. If the key is not
// found, [ErrNotFound] is returned as the error value.
func Del(ctx context.Context, blocks block.Fetcher, root ipld.Link, key string) (ipld.Link, shard.Diff, error) {
	shards := shard.NewFetcher(blocks)
	rshard, err := shards.GetRoot(ctx, root)
	if err != nil {
		return nil, shard.Diff{}, err
	}

	path, err := traverse(ctx, shards, shard.AsBlock(rshard), key)
	if err != nil {
		return nil, shard.Diff{}, fmt.Errorf("traversing shard: %w", err)
	}
	target := path[len(path)-1]
	skey := key[len(target.Value().Prefix()):]

	entryidx := slices.IndexFunc(target.Value().Entries(), func(e shard.Entry) bool {
		return e.Key() == skey
	})
	if entryidx == -1 {
		return nil, shard.Diff{}, ErrNotFound
	}

	entry := target.Value().Entries()[entryidx]
	// cannot delete a shard (without data)
	if entry.Value().Value() == nil {
		return nil, shard.Diff{}, ErrNotFound
	}

	var additions []shard.BlockView
	removals := path[:]

	var nshard shard.Shard
	if entry.Value().Shard() != nil {
		// remove the value from this link+value
		ents := target.Value().Entries()[:]
		ents[entryidx] = shard.NewEntry(entry.Key(), shard.NewValue(nil, entry.Value().Shard()))
		if target.Value().Prefix() == "" {
			nshard = shard.NewRoot(ents)
		} else {
			nshard = shard.New(target.Value().Prefix(), ents)
		}
	} else {
		ents := slices.Delete(target.Value().Entries()[:], entryidx, entryidx+1)
		if target.Value().Prefix() == "" {
			nshard = shard.NewRoot(ents)
		} else {
			nshard = shard.New(target.Value().Prefix(), ents)
		}

		// if now empty, remove from parent
		for i := len(path) - 1; i > 0; i-- {
			if len(nshard.Entries()) > 0 {
				break
			}

			child := path[i]
			parent := path[i-1]

			entidx := slices.IndexFunc(parent.Value().Entries(), func(e shard.Entry) bool {
				return e.Value().Shard() != nil && e.Value().Shard().String() == child.Link().String()
			})
			if entidx == -1 { // should not happen!
				return nil, shard.Diff{}, errors.New("did not find child shard link in parent")
			}

			// delete the parent entry, unless it has a value also, then just clear
			// the shard link.
			var ents []shard.Entry
			if parent.Value().Entries()[entidx].Value().Value() != nil {
				ents = parent.Value().Entries()[:]
				ents[entidx] = shard.NewEntry(ents[entidx].Key(), shard.NewValue(ents[entidx].Value().Value(), nil))
			} else {
				ents = slices.Delete(parent.Value().Entries()[:], entidx, 1)
			}

			nshard = shard.New(parent.Value().Prefix(), ents)
			path = path[:i] // pop the parent from the path, they no longer exist
		}
	}

	child, err := shard.MarshalBlock(nshard)
	if err != nil {
		return nil, shard.Diff{}, err
	}
	additions = append(additions, child)

	// path is root -> target, so work backwards, propagating the new shard CID
	for i := len(path) - 2; i >= 0; i-- {
		parent := path[i]
		key := child.Value().Prefix()[len(parent.Value().Prefix()):]

		entries := parent.Value().Entries()[:]
		for i, e := range entries {
			if e.Key() == key {
				if e.Value().Shard() == nil {
					return nil, shard.Diff{}, fmt.Errorf("\"%s\" is not a shard link in: %s", key, parent.Link().String())
				}
				entries[i] = shard.NewEntry(key, shard.NewValue(e.Value().Value(), child.Link()))
				break
			}
		}

		var cshard shard.Shard
		if parent.Value().Prefix() == "" {
			cshard = shard.NewRoot(entries)
		} else {
			cshard = shard.New(parent.Value().Prefix(), entries)
		}

		child, err = shard.MarshalBlock(cshard)
		if err != nil {
			return nil, shard.Diff{}, err
		}
		additions = append(additions, child)
	}

	return additions[len(additions)-1].Link(), shard.Diff{Additions: additions, Removals: removals}, nil
}
