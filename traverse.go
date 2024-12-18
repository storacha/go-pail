package pail

import (
	"context"
	"fmt"
	"strings"

	"github.com/storacha/go-pail/shard"
)

// Traverse from the passed shard block to the target shard block using the
// passed key. All traversed shards are returned, starting with the passed shard
// and ending with the target.
func traverse(ctx context.Context, shards *shard.Fetcher, shardBlock shard.BlockView, key string) ([]shard.BlockView, error) {
	for _, e := range shardBlock.Value().Entries() {
		k := e.Key()
		v := e.Value()
		if key == k {
			break
		}
		if strings.HasPrefix(key, k) && v.Shard() != nil {
			s, err := shards.Get(ctx, v.Shard())
			if err != nil {
				return nil, fmt.Errorf("getting shard %s: %w", v.Shard().String(), err)
			}
			path, err := traverse(ctx, shards, s, key[len(k):])
			if err != nil {
				return nil, err
			}
			return append([]shard.BlockView{shardBlock}, path...), nil
		}
	}
	return []shard.BlockView{shardBlock}, nil
}
