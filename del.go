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
func Del(ctx context.Context, blocks block.Fetcher, root ipld.Link, key string) (ipld.Link, ShardDiff, error) {
	shards := shard.NewFetcher(blocks)
	rshard, err := shards.GetRoot(ctx, root)
	if err != nil {
		return nil, ShardDiff{}, err
	}

	path, err := traverse(ctx, shards, shard.AsBlock(rshard), key)
	if err != nil {
		return nil, ShardDiff{}, fmt.Errorf("traversing shard: %w", err)
	}
	target := path[len(path)-1]
	skey := key[len(target.Value().Prefix()):]

	entryidx := slices.IndexFunc(target.Value().Entries(), func(e shard.Entry) bool {
		return e.Key() == skey
	})
	if entryidx == -1 {
		return nil, ShardDiff{}, ErrNotFound
	}

	entry := target.Value().Entries()[entryidx]
	// cannot delete a shard (without data)
	if entry.Value().Value() == nil {
		return nil, ShardDiff{}, ErrNotFound
	}

	// var additions []shard.BlockView
	// removals := path[:]

	// nshard := shard.New(target.Value().Prefix(), target.Value().Entries()[:])
	// TODO TODO
	return nil, ShardDiff{}, errors.New("not implemented")
}
