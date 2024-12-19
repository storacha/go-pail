package pail

import (
	"context"
	"testing"

	"github.com/storacha/go-pail/internal/testutil"
	"github.com/storacha/go-pail/shard"
	"github.com/stretchr/testify/require"
)

func TestDel(t *testing.T) {
	ctx := context.Background()

	t.Run("del from root shard", func(t *testing.T) {
		rb0, err := shard.EncodeBlock(shard.NewRoot(nil))
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

		v1, err := Get(ctx, bs, r1, k0)
		require.NoError(t, err)
		require.Equal(t, v0, v1)

		r2, diff1, err := Del(ctx, bs, r1, k0)
		require.NoError(t, err)

		testutil.ApplyDiff(t, diff1, bs)

		_, err = Get(ctx, bs, r2, k0)
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("del from shard", func(t *testing.T) {
		rb0, err := shard.EncodeBlock(shard.NewRoot(nil))
		require.NoError(t, err)

		bs := testutil.NewBlockstore()
		err = bs.Put(ctx, rb0)
		require.NoError(t, err)

		r0 := rb0.Link()
		v0 := testutil.RandomLink(t)
		v1 := testutil.RandomLink(t)
		v2 := testutil.RandomLink(t)
		r1 := putAll(t, bs, r0, []object{
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
		}, materialize(t, bs, r1))

		r2, diff1, err := Del(ctx, bs, r1, "aabb")
		require.NoError(t, err)

		testutil.ApplyDiff(t, diff1, bs)

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
								},
							},
						},
					},
				},
			},
		}, materialize(t, bs, r2))
	})

	t.Run("del from shard link", func(t *testing.T) {
		rb0, err := shard.EncodeBlock(shard.NewRoot(nil))
		require.NoError(t, err)

		bs := testutil.NewBlockstore()
		err = bs.Put(ctx, rb0)
		require.NoError(t, err)

		r0 := rb0.Link()
		v0 := testutil.RandomLink(t)
		v1 := testutil.RandomLink(t)
		v2 := testutil.RandomLink(t)
		r1 := putAll(t, bs, r0, []object{
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
		}, materialize(t, bs, r1))

		r2, diff1, err := Del(ctx, bs, r1, "aa")
		require.NoError(t, err)

		testutil.ApplyDiff(t, diff1, bs)

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
		}, materialize(t, bs, r2))
	})

	t.Run("del from shard link in root shard", func(t *testing.T) {
		rb0, err := shard.EncodeBlock(shard.NewRoot(nil))
		require.NoError(t, err)

		bs := testutil.NewBlockstore()
		err = bs.Put(ctx, rb0)
		require.NoError(t, err)

		r0 := rb0.Link()
		v0 := testutil.RandomLink(t)
		v1 := testutil.RandomLink(t)
		r1 := putAll(t, bs, r0, []object{
			{"a", v0},
			{"aabb", v1},
		})

		require.Equal(t, []entry{
			{
				"a",
				value{
					v0,
					[]entry{
						{
							"abb",
							value{v1, nil},
						},
					},
				},
			},
		}, materialize(t, bs, r1))

		r2, diff1, err := Del(ctx, bs, r1, "a")
		require.NoError(t, err)

		testutil.ApplyDiff(t, diff1, bs)

		require.Equal(t, []entry{
			{
				"a",
				value{
					nil,
					[]entry{
						{
							"abb",
							value{v1, nil},
						},
					},
				},
			},
		}, materialize(t, bs, r2))
	})

	t.Run("del from shard does not remove shard link value", func(t *testing.T) {
		rb0, err := shard.EncodeBlock(shard.NewRoot(nil))
		require.NoError(t, err)

		bs := testutil.NewBlockstore()
		err = bs.Put(ctx, rb0)
		require.NoError(t, err)

		r0 := rb0.Link()
		v0 := testutil.RandomLink(t)
		v1 := testutil.RandomLink(t)
		r1 := putAll(t, bs, r0, []object{
			{"aaaa", v0},
			{"aa", v1},
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
								v1,
								[]entry{
									{
										"aa",
										value{v0, nil},
									},
								},
							},
						},
					},
				},
			},
		}, materialize(t, bs, r1))

		r2, diff1, err := Del(ctx, bs, r1, "aaaa")
		require.NoError(t, err)

		testutil.ApplyDiff(t, diff1, bs)

		require.Equal(t, []entry{
			{
				"a",
				value{
					nil,
					[]entry{
						{
							"a",
							value{
								v1,
								nil,
							},
						},
					},
				},
			},
		}, materialize(t, bs, r2))
	})

	t.Run("del from shard does not remove shard link value 2", func(t *testing.T) {
		rb0, err := shard.EncodeBlock(shard.NewRoot(nil))
		require.NoError(t, err)

		bs := testutil.NewBlockstore()
		err = bs.Put(ctx, rb0)
		require.NoError(t, err)

		r0 := rb0.Link()
		v0 := testutil.RandomLink(t)
		v1 := testutil.RandomLink(t)
		v2 := testutil.RandomLink(t)
		r1 := putAll(t, bs, r0, []object{
			{"aaaa", v0},
			{"aa", v1},
			{"aaaaA", v2},
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
								v1,
								[]entry{
									{
										"a",
										value{
											nil,
											[]entry{
												{
													"a",
													value{
														v0,
														[]entry{
															{
																"A",
																value{v2, nil},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}, materialize(t, bs, r1))

		r2, diff1, err := Del(ctx, bs, r1, "aaaa")
		require.NoError(t, err)

		testutil.ApplyDiff(t, diff1, bs)

		r3, diff1, err := Del(ctx, bs, r2, "aaaaA")
		require.NoError(t, err)

		testutil.ApplyDiff(t, diff1, bs)

		require.Equal(t, []entry{
			{
				"a",
				value{
					nil,
					[]entry{
						{
							"a",
							value{v1, nil},
						},
					},
				},
			},
		}, materialize(t, bs, r3))
	})
}
