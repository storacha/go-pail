package pail

import (
	"context"
	"errors"
	"testing"

	"github.com/storacha/go-pail/internal/testutil"
	"github.com/storacha/go-pail/shard"
	"github.com/stretchr/testify/require"
)

func TestGet(t *testing.T) {
	ctx := context.Background()

	t.Run("get from root shard", func(t *testing.T) {
		rb0, err := shard.MarshalBlock(shard.NewRoot(nil))
		require.NoError(t, err)

		bs := testutil.NewBlockstore()
		err = bs.Put(ctx, rb0)
		require.NoError(t, err)

		r0 := rb0.Link()
		k0 := "test"
		v0 := testutil.RandomLink(t)

		r1, diff0, err := Put(ctx, bs, r0, k0, v0)
		require.NoError(t, err)

		for _, b := range diff0.Additions {
			err = bs.Put(ctx, b)
			require.NoError(t, err)
		}
		for _, b := range diff0.Removals {
			err = bs.Del(ctx, b.Link())
			require.NoError(t, err)
		}

		o0, err := Get(ctx, bs, r1, k0)
		require.NoError(t, err)
		require.Equal(t, v0, o0)
	})

	t.Run("get from shard", func(t *testing.T) {
		rb0, err := shard.MarshalBlock(shard.NewRoot(nil))
		require.NoError(t, err)

		bs := testutil.NewBlockstore()
		err = bs.Put(ctx, rb0)
		require.NoError(t, err)

		r0 := rb0.Link()
		k0 := "aaaa"
		v0 := testutil.RandomLink(t)

		r1, diff0, err := Put(ctx, bs, r0, k0, v0)
		require.NoError(t, err)

		for _, b := range diff0.Additions {
			err = bs.Put(ctx, b)
			require.NoError(t, err)
		}
		for _, b := range diff0.Removals {
			err = bs.Del(ctx, b.Link())
			require.NoError(t, err)
		}

		k1 := "aaab"
		v1 := testutil.RandomLink(t)

		r2, diff1, err := Put(ctx, bs, r1, k1, v1)
		require.NoError(t, err)

		for _, b := range diff1.Additions {
			err = bs.Put(ctx, b)
			require.NoError(t, err)
		}
		for _, b := range diff1.Removals {
			err = bs.Del(ctx, b.Link())
			require.NoError(t, err)
		}

		o0, err := Get(ctx, bs, r2, k1)
		require.NoError(t, err)
		require.Equal(t, v1, o0)
	})

	t.Run("get from shard link", func(t *testing.T) {
		rb0, err := shard.MarshalBlock(shard.NewRoot(nil))
		require.NoError(t, err)

		bs := testutil.NewBlockstore()
		err = bs.Put(ctx, rb0)
		require.NoError(t, err)

		r0 := rb0.Link()
		k0 := "aaaa"
		v0 := testutil.RandomLink(t)

		r1, diff0, err := Put(ctx, bs, r0, k0, v0)
		require.NoError(t, err)

		for _, b := range diff0.Additions {
			err = bs.Put(ctx, b)
			require.NoError(t, err)
		}
		for _, b := range diff0.Removals {
			err = bs.Del(ctx, b.Link())
			require.NoError(t, err)
		}

		k1 := "aaa"
		v1 := testutil.RandomLink(t)

		r2, diff1, err := Put(ctx, bs, r1, k1, v1)
		require.NoError(t, err)

		for _, b := range diff1.Additions {
			err = bs.Put(ctx, b)
			require.NoError(t, err)
		}
		for _, b := range diff1.Removals {
			err = bs.Del(ctx, b.Link())
			require.NoError(t, err)
		}

		o0, err := Get(ctx, bs, r2, k1)
		require.NoError(t, err)
		require.Equal(t, v1, o0)
	})

	t.Run("not found", func(t *testing.T) {
		rb0, err := shard.MarshalBlock(shard.NewRoot(nil))
		require.NoError(t, err)

		bs := testutil.NewBlockstore()
		err = bs.Put(ctx, rb0)
		require.NoError(t, err)

		r0 := rb0.Link()
		k0 := "test"

		_, err = Get(ctx, bs, r0, k0)
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrNotFound))
	})
}
