package pail

import (
	"context"
	"testing"

	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-pail/block"
	"github.com/storacha/go-pail/internal/testutil"
	"github.com/storacha/go-pail/shard"
	"github.com/stretchr/testify/require"
)

func TestPut(t *testing.T) {
	ctx := context.Background()

	t.Run("put to empty shard", func(t *testing.T) {
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

		require.Len(t, diff0.Removals, 1)
		require.Equal(t, r0.String(), diff0.Removals[0].Link().String())
		require.Len(t, diff0.Additions, 1)
		require.Equal(t, r1.String(), diff0.Additions[0].Link().String())
		require.Len(t, diff0.Additions[0].Value().Entries(), 1)
		require.Equal(t, k0, diff0.Additions[0].Value().Entries()[0].Key())
		require.Equal(t, v0.String(), diff0.Additions[0].Value().Entries()[0].Value().Value().String())
	})

	t.Run("put same value to existing key", func(t *testing.T) {
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

		testutil.ApplyDiff(t, diff0, bs)

		r2, diff1, err := Put(ctx, bs, r1, k0, v0)
		require.NoError(t, err)
		require.Equal(t, r1.String(), r2.String())
		require.Len(t, diff1.Additions, 0)
		require.Len(t, diff1.Removals, 0)
	})

	t.Run("auto-shards on similar key", func(t *testing.T) {
		rb0, err := shard.MarshalBlock(shard.NewRoot(nil))
		require.NoError(t, err)

		bs := testutil.NewBlockstore()
		err = bs.Put(ctx, rb0)
		require.NoError(t, err)

		v0 := testutil.RandomLink(t)
		v1 := testutil.RandomLink(t)
		r0 := putAll(t, bs, rb0.Link(), []object{{"aaaa", v0}, {"aabb", v1}})

		require.Equal(t, []entry{
			{
				"a",
				value{
					nil,
					[]entry{
						{
							"a",
							value{
								nil,
								[]entry{
									{
										"aa",
										value{v0, nil},
									},
									{
										"bb",
										value{v1, nil},
									},
								},
							},
						},
					},
				},
			},
		}, materialize(t, bs, r0))
	})

	t.Run("put to shard link", func(t *testing.T) {
		rb0, err := shard.MarshalBlock(shard.NewRoot(nil))
		require.NoError(t, err)

		bs := testutil.NewBlockstore()
		err = bs.Put(ctx, rb0)
		require.NoError(t, err)

		v0 := testutil.RandomLink(t)
		v1 := testutil.RandomLink(t)
		v2 := testutil.RandomLink(t)
		r0 := putAll(t, bs, rb0.Link(), []object{
			{"aaaa", v0},
			{"aabb", v1},
			{"aa", v2},
		})

		require.Equal(t, []entry{
			{
				"a",
				value{
					nil,
					[]entry{
						{
							"a",
							value{
								v2,
								[]entry{
									{
										"aa",
										value{v0, nil},
									},
									{
										"bb",
										value{v1, nil},
									},
								},
							},
						},
					},
				},
			},
		}, materialize(t, bs, r0))
	})

	t.Run("deterministic structure", func(t *testing.T) {
		v0 := testutil.RandomLink(t)
		v1 := testutil.RandomLink(t)
		v2 := testutil.RandomLink(t)
		v3 := testutil.RandomLink(t)
		v4 := testutil.RandomLink(t)

		objects := []object{
			{"aaaa", v0},
			{"aaab", v1},
			{"aabb", v2},
			{"abbb", v3},
			{"bbbb", v4},
		}

		orders := [][]int{
			{0, 1, 2, 3, 4},
			{4, 3, 2, 1, 0},
			{1, 2, 4, 0, 3},
			{2, 0, 3, 4, 1},
		}

		for _, order := range orders {
			rb0, err := shard.MarshalBlock(shard.NewRoot(nil))
			require.NoError(t, err)

			bs := testutil.NewBlockstore()
			err = bs.Put(ctx, rb0)
			require.NoError(t, err)

			var objs []object
			for _, i := range order {
				objs = append(objs, objects[i])
			}

			r0 := putAll(t, bs, rb0.Link(), objs)

			require.Equal(t, []entry{
				{
					"a",
					value{
						nil,
						[]entry{
							{
								"a",
								value{
									nil,
									[]entry{
										{
											"a",
											value{
												nil,
												[]entry{
													{
														"a",
														value{v0, nil},
													},
													{
														"b",
														value{v1, nil},
													},
												},
											},
										},
										{
											"bb",
											value{v2, nil},
										},
									},
								},
							},
							{
								"bbb",
								value{v3, nil},
							},
						},
					},
				},
				{
					"bbbb",
					value{v4, nil},
				},
			}, materialize(t, bs, r0))
		}
	})
}

type entry struct {
	key   string
	value value
}

type value struct {
	value   ipld.Link
	entries []entry
}

type object struct {
	key   string
	value ipld.Link
}

// materialize creates a fully constructed in memory pail with inlined shards.
func materialize(t *testing.T, blocks block.Fetcher, root ipld.Link) []entry {
	ctx := context.Background()
	shards := shard.NewFetcher(blocks)
	s, err := shards.Get(ctx, root)
	require.NoError(t, err)

	var entries []entry
	for _, e := range s.Value().Entries() {
		var cents []entry
		if e.Value().Shard() != nil {
			cents = materialize(t, blocks, e.Value().Shard())
		}
		entry := entry{e.Key(), value{value: e.Value().Value(), entries: cents}}
		entries = append(entries, entry)
	}
	return entries
}

func putAll(t *testing.T, bs testutil.Blockstore, root ipld.Link, objects []object) ipld.Link {
	ctx := context.Background()
	for _, o := range objects {
		r, diff, err := Put(ctx, bs, root, o.key, o.value)
		require.NoError(t, err)
		testutil.ApplyDiff(t, diff, bs)
		root = r
	}
	return root
}
