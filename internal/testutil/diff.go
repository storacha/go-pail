package testutil

import (
	"context"
	"testing"

	"github.com/storacha/go-pail/shard"
	"github.com/stretchr/testify/require"
)

func ApplyDiff(t *testing.T, diff shard.Diff, bs Blockstore) {
	var err error
	for _, b := range diff.Additions {
		err = bs.Put(context.Background(), b)
		require.NoError(t, err)
	}
	for _, b := range diff.Removals {
		err = bs.Del(context.Background(), b.Link())
		require.NoError(t, err)
	}
}
