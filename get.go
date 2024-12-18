package pail

import (
	"context"
	"errors"

	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-pail/block"
	"github.com/storacha/go-pail/shard"
)

var ErrNotFound = errors.New("not found")

// Get the stored value for the given key from the bucket. If the key is not
// found, [ErrNotFound] is returned as the error value.
func Get(ctx context.Context, blocks block.Fetcher, root ipld.Link, key string) (ipld.Link, error) {
	shards := shard.NewFetcher(blocks)
	rshard, err := shards.GetRoot(ctx, root)
	if err != nil {
		return nil, err
	}

	path, err := traverse(ctx, shards, shard.AsBlock(rshard), key)
	if err != nil {
		return nil, err
	}

	target := path[len(path)-1]
	skey := key[len(target.Value().Prefix()):] // key within the shard
	var entry shard.Entry
	for _, e := range target.Value().Entries() {
		if e.Key() == skey {
			entry = e
			break
		}
	}
	if entry == nil {
		return nil, ErrNotFound
	}
	return entry.Value().Value(), nil
}
