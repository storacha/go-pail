package pail

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/storacha/go-pail/internal/testutil"
	"github.com/storacha/go-pail/shard"
	"github.com/stretchr/testify/require"
)

func TestEntries(t *testing.T) {
	ctx := context.Background()

	t.Run("lists entries in lexicographical order", func(t *testing.T) {
		rb0, err := shard.EncodeBlock(shard.NewRoot(nil))
		require.NoError(t, err)

		bs := testutil.NewBlockstore()
		err = bs.Put(ctx, rb0)
		require.NoError(t, err)

		r0 := rb0.Link()
		objects := []object{
			{"c", testutil.RandomLink(t)},
			{"d", testutil.RandomLink(t)},
			{"a", testutil.RandomLink(t)},
			{"b", testutil.RandomLink(t)},
		}
		r1 := putAll(t, bs, r0, objects)

		var results []Entry
		for e, err := range Entries(ctx, bs, r1) {
			require.NoError(t, err)
			results = append(results, e)
		}
		require.Len(t, results, len(objects))

		slices.SortFunc(objects, objectKeySort)

		for i, o := range objects {
			require.Equal(t, o.key, results[i].Key)
			require.Equal(t, o.value.String(), results[i].Value.String())
		}
	})

	t.Run("lists entries by prefix", func(t *testing.T) {
		rb0, err := shard.EncodeBlock(shard.NewRoot(nil))
		require.NoError(t, err)

		bs := testutil.NewBlockstore()
		err = bs.Put(ctx, rb0)
		require.NoError(t, err)

		r0 := rb0.Link()
		objects := []object{
			{"ceee", testutil.RandomLink(t)},
			{"deee", testutil.RandomLink(t)},
			{"dooo", testutil.RandomLink(t)},
			{"beee", testutil.RandomLink(t)},
		}
		r1 := putAll(t, bs, r0, objects)

		pfx := "d"
		var expectObjs []object
		for _, o := range objects {
			if strings.HasPrefix(o.key, pfx) {
				expectObjs = append(expectObjs, o)
			}
		}
		slices.SortFunc(expectObjs, objectKeySort)

		var results []Entry
		for e, err := range Entries(ctx, bs, r1, WithKeyPrefix(pfx)) {
			require.NoError(t, err)
			results = append(results, e)
		}
		require.Len(t, results, len(expectObjs))

		for i, o := range expectObjs {
			require.Equal(t, o.key, results[i].Key)
			require.Equal(t, o.value.String(), results[i].Value.String())
		}
	})

	t.Run("lists entries by key greater than", func(t *testing.T) {
		rb0, err := shard.EncodeBlock(shard.NewRoot(nil))
		require.NoError(t, err)

		bs := testutil.NewBlockstore()
		err = bs.Put(ctx, rb0)
		require.NoError(t, err)

		r0 := rb0.Link()
		objects := []object{
			{"ceee", testutil.RandomLink(t)},
			{"deee", testutil.RandomLink(t)},
			{"dooo", testutil.RandomLink(t)},
			{"beee", testutil.RandomLink(t)},
		}
		r1 := putAll(t, bs, r0, objects)

		gt := "beee"
		var expectObjs []object
		for _, o := range objects {
			if o.key > gt {
				expectObjs = append(expectObjs, o)
			}
		}
		slices.SortFunc(expectObjs, objectKeySort)

		var results []Entry
		for e, err := range Entries(ctx, bs, r1, WithKeyGreaterThan(gt)) {
			require.NoError(t, err)
			results = append(results, e)
		}
		require.Len(t, results, len(expectObjs))

		for i, o := range expectObjs {
			require.Equal(t, o.key, results[i].Key)
			require.Equal(t, o.value.String(), results[i].Value.String())
		}
	})

	t.Run("lists entries by key greater than or equal", func(t *testing.T) {
		rb0, err := shard.EncodeBlock(shard.NewRoot(nil))
		require.NoError(t, err)

		bs := testutil.NewBlockstore()
		err = bs.Put(ctx, rb0)
		require.NoError(t, err)

		r0 := rb0.Link()
		objects := []object{
			{"ceee", testutil.RandomLink(t)},
			{"deee", testutil.RandomLink(t)},
			{"dooo", testutil.RandomLink(t)},
			{"beee", testutil.RandomLink(t)},
		}
		r1 := putAll(t, bs, r0, objects)

		gte := "beee"
		var expectObjs []object
		for _, o := range objects {
			if o.key >= gte {
				expectObjs = append(expectObjs, o)
			}
		}
		slices.SortFunc(expectObjs, objectKeySort)

		var results []Entry
		for e, err := range Entries(ctx, bs, r1, WithKeyGreaterThanOrEqual(gte)) {
			require.NoError(t, err)
			results = append(results, e)
		}
		require.Len(t, results, len(expectObjs))

		for i, o := range expectObjs {
			require.Equal(t, o.key, results[i].Key)
			require.Equal(t, o.value.String(), results[i].Value.String())
		}
	})

	t.Run("lists entries by key less than", func(t *testing.T) {
		rb0, err := shard.EncodeBlock(shard.NewRoot(nil))
		require.NoError(t, err)

		bs := testutil.NewBlockstore()
		err = bs.Put(ctx, rb0)
		require.NoError(t, err)

		r0 := rb0.Link()
		objects := []object{
			{"ceee", testutil.RandomLink(t)},
			{"deee", testutil.RandomLink(t)},
			{"dooo", testutil.RandomLink(t)},
			{"beee", testutil.RandomLink(t)},
		}
		r1 := putAll(t, bs, r0, objects)

		lt := "doo"
		var expectObjs []object
		for _, o := range objects {
			if o.key < lt {
				expectObjs = append(expectObjs, o)
			}
		}
		slices.SortFunc(expectObjs, objectKeySort)

		var results []Entry
		for e, err := range Entries(ctx, bs, r1, WithKeyLessThan(lt)) {
			require.NoError(t, err)
			results = append(results, e)
		}
		require.Len(t, results, len(expectObjs))

		for i, o := range expectObjs {
			require.Equal(t, o.key, results[i].Key)
			require.Equal(t, o.value.String(), results[i].Value.String())
		}
	})

	t.Run("lists entries by key less than or equal", func(t *testing.T) {
		rb0, err := shard.EncodeBlock(shard.NewRoot(nil))
		require.NoError(t, err)

		bs := testutil.NewBlockstore()
		err = bs.Put(ctx, rb0)
		require.NoError(t, err)

		r0 := rb0.Link()
		objects := []object{
			{"ceee", testutil.RandomLink(t)},
			{"deee", testutil.RandomLink(t)},
			{"dooo", testutil.RandomLink(t)},
			{"beee", testutil.RandomLink(t)},
		}
		r1 := putAll(t, bs, r0, objects)

		lte := "doo"
		var expectObjs []object
		for _, o := range objects {
			if o.key <= lte {
				expectObjs = append(expectObjs, o)
			}
		}
		slices.SortFunc(expectObjs, objectKeySort)

		var results []Entry
		for e, err := range Entries(ctx, bs, r1, WithKeyLessThanOrEqual(lte)) {
			require.NoError(t, err)
			results = append(results, e)
		}
		require.Len(t, results, len(expectObjs))

		for i, o := range expectObjs {
			require.Equal(t, o.key, results[i].Key)
			require.Equal(t, o.value.String(), results[i].Value.String())
		}
	})

	t.Run("lists entries by key greater than and less than or equal to", func(t *testing.T) {
		rb0, err := shard.EncodeBlock(shard.NewRoot(nil))
		require.NoError(t, err)

		bs := testutil.NewBlockstore()
		err = bs.Put(ctx, rb0)
		require.NoError(t, err)

		r0 := rb0.Link()
		objects := []object{
			{"ceee", testutil.RandomLink(t)},
			{"deee", testutil.RandomLink(t)},
			{"dooo", testutil.RandomLink(t)},
			{"beee", testutil.RandomLink(t)},
		}
		r1 := putAll(t, bs, r0, objects)

		gt := "c"
		lte := "deee"
		var expectObjs []object
		for _, o := range objects {
			if o.key > gt && o.key <= lte {
				expectObjs = append(expectObjs, o)
			}
		}
		slices.SortFunc(expectObjs, objectKeySort)

		var results []Entry
		for e, err := range Entries(ctx, bs, r1, WithKeyGreaterThan(gt), WithKeyLessThanOrEqual(lte)) {
			require.NoError(t, err)
			results = append(results, e)
		}
		require.Len(t, results, len(expectObjs))

		for i, o := range expectObjs {
			require.Equal(t, o.key, results[i].Key)
			require.Equal(t, o.value.String(), results[i].Value.String())
		}
	})
}

func objectKeySort(a, b object) int {
	if a.key < b.key {
		return -1
	} else if a.key > b.key {
		return 1
	}
	return 0
}
