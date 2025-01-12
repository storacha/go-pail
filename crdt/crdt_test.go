package crdt

import (
	"context"
	"fmt"
	"iter"
	"slices"
	"testing"

	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-pail"
	"github.com/storacha/go-pail/clock"
	"github.com/storacha/go-pail/crdt/operation"
	"github.com/storacha/go-pail/internal/testutil"
	"github.com/storacha/go-pail/ipld/node"
	"github.com/stretchr/testify/require"
)

func TestCRDT(t *testing.T) {
	ctx := context.Background()

	t.Run("put a value to a new clock", func(t *testing.T) {
		bs := testutil.NewBlockstore()
		alice := testPail{t: t, blocks: bs}

		key := "key"
		val := testutil.RandomLink(t)
		res := alice.Put(ctx, key, val)

		alice.Visualize(ctx)

		require.NotNil(t, res.Event)
		require.Equal(t, operation.TypePut, res.Event.Value().Data().Type())
		require.Equal(t, key, res.Event.Value().Data().Key())
		require.Equal(t, val, res.Event.Value().Data().Value())
		require.Len(t, res.Head, 1)
		require.Equal(t, res.Event.Link(), res.Head[0])
	})

	t.Run("linear put multiple values", func(t *testing.T) {
		bs := testutil.NewBlockstore()
		alice := testPail{t: t, blocks: bs}

		key0 := "key0"
		val0 := testutil.RandomLink(t)
		alice.Put(ctx, key0, val0)

		key1 := "key1"
		val1 := testutil.RandomLink(t)
		res := alice.Put(ctx, key1, val1)

		alice.Visualize(ctx)

		require.NotNil(t, res.Event)
		require.Equal(t, operation.TypePut, res.Event.Value().Data().Type())
		require.Equal(t, key1, res.Event.Value().Data().Key())
		require.Equal(t, val1, res.Event.Value().Data().Value())
		require.Len(t, res.Head, 1)
		require.Equal(t, res.Event.Link(), res.Head[0])
	})

	t.Run("simple parallel put multiple values", func(t *testing.T) {
		bs := testutil.NewBlockstore()
		alice := testPail{t: t, blocks: bs}

		apple := pail.Entry{Key: "apple", Value: testutil.RandomLink(t)}
		alice.Put(ctx, apple.Key, apple.Value)

		bob := testPail{t: t, blocks: bs, head: alice.head}
		data := []pail.Entry{
			{Key: "banana", Value: testutil.RandomLink(t)},
			{Key: "kiwi", Value: testutil.RandomLink(t)},
			{Key: "mango", Value: testutil.RandomLink(t)},
			{Key: "orange", Value: testutil.RandomLink(t)},
			{Key: "pear", Value: testutil.RandomLink(t)},
		}

		ar0 := alice.Put(ctx, data[0].Key, data[0].Value)
		br0 := bob.Put(ctx, data[1].Key, data[1].Value)

		require.NotNil(t, ar0.Event)
		require.NotNil(t, br0.Event)

		carol := testPail{t: t, blocks: bs, head: bob.head}

		br1 := bob.Put(ctx, data[2].Key, data[2].Value)
		cr0 := carol.Put(ctx, data[3].Key, data[3].Value)

		require.NotNil(t, br1.Event)
		require.NotNil(t, cr0.Event)

		alice.Advance(ctx, cr0.Event.Link())
		alice.Advance(ctx, br0.Event.Link())
		alice.Advance(ctx, br1.Event.Link())
		bob.Advance(ctx, ar0.Event.Link())

		ar1 := alice.Put(ctx, data[4].Key, data[4].Value)
		alice.Visualize(ctx)

		require.NotNil(t, ar1.Event)

		bob.Advance(ctx, ar1.Event.Link())
		carol.Advance(ctx, ar1.Event.Link())

		require.Equal(t, alice.root, bob.root)
		require.Equal(t, alice.root, carol.root)

		// get item put to bob from alice
		avalue, err := alice.Get(ctx, data[1].Key)
		require.NoError(t, err)
		require.Equal(t, data[1].Value, avalue)

		// get item put to alice from bob
		bvalue, err := bob.Get(ctx, data[0].Key)
		require.NoError(t, err)
		require.Equal(t, data[0].Value, bvalue)

		// get item put to alice from carol
		cvalue, err := bob.Get(ctx, data[4].Key)
		require.NoError(t, err)
		require.Equal(t, data[4].Value, cvalue)
	})

	t.Run("get from multi event head", func(t *testing.T) {
		bs := testutil.NewBlockstore()
		alice := testPail{t: t, blocks: bs}

		apple := pail.Entry{Key: "apple", Value: testutil.RandomLink(t)}
		alice.Put(ctx, apple.Key, apple.Value)

		bob := testPail{t: t, blocks: bs, head: alice.head}
		data := []pail.Entry{
			{Key: "banana", Value: testutil.RandomLink(t)},
			{Key: "kiwi", Value: testutil.RandomLink(t)},
		}

		alice.Put(ctx, data[0].Key, data[0].Value)
		res := bob.Put(ctx, data[1].Key, data[1].Value)

		require.NotNil(t, res.Event)

		_, err := alice.Get(ctx, data[1].Key)
		require.Error(t, err)
		require.ErrorIs(t, err, pail.ErrNotFound)

		alice.Advance(ctx, res.Event.Link())

		value, err := alice.Get(ctx, data[1].Key)
		require.NoError(t, err)
		require.Equal(t, data[1].Value, value)
	})

	t.Run("entries from multi event head", func(t *testing.T) {
		bs := testutil.NewBlockstore()
		alice := testPail{t: t, blocks: bs}

		apple := pail.Entry{Key: "apple", Value: testutil.RandomLink(t)}
		alice.Put(ctx, apple.Key, apple.Value)

		bob := testPail{t: t, blocks: bs, head: alice.head}
		data := []pail.Entry{
			{Key: "banana", Value: testutil.RandomLink(t)},
			{Key: "kiwi", Value: testutil.RandomLink(t)},
		}

		alice.Put(ctx, data[0].Key, data[0].Value)
		res := bob.Put(ctx, data[1].Key, data[1].Value)

		require.NotNil(t, res.Event)

		// alice has only apple and banana
		objs := slices.Collect(alice.Entries(ctx))
		require.Equal(t, []pail.Entry{apple, data[0]}, objs)

		alice.Advance(ctx, res.Event.Link())

		objs = slices.Collect(alice.Entries(ctx))
		require.Equal(t, []pail.Entry{apple, data[0], data[1]}, objs)
	})
}

type testPail struct {
	t      *testing.T
	blocks *testutil.MapBlockstore
	head   []ipld.Link
	root   ipld.Link
}

func (tp *testPail) Advance(ctx context.Context, event ipld.Link) []ipld.Link {
	head, err := clock.Advance(ctx, tp.blocks, node.BinderFunc[operation.Operation](operation.Bind), tp.head, event)
	require.NoError(tp.t, err)

	root, diff, err := Root(ctx, tp.blocks, head)
	require.NoError(tp.t, err)

	for _, b := range diff.Additions {
		err = tp.blocks.Put(ctx, b)
		require.NoError(tp.t, err)
	}

	tp.head = head
	tp.root = root

	return head
}

func (tp *testPail) Put(ctx context.Context, key string, value ipld.Link) Result {
	res, err := Put(ctx, tp.blocks, tp.head, key, value)
	require.NoError(tp.t, err)

	if res.Event != nil {
		err := tp.blocks.Put(ctx, res.Event)
		require.NoError(tp.t, err)
	}

	for _, b := range res.Additions {
		err = tp.blocks.Put(ctx, b)
		require.NoError(tp.t, err)
	}

	tp.head = res.Head
	tp.root = res.Root

	return res
}

func (tp *testPail) Get(ctx context.Context, key string) (ipld.Link, error) {
	return Get(ctx, tp.blocks, tp.head, key)
}

func (tp *testPail) Entries(ctx context.Context) iter.Seq[pail.Entry] {
	return func(yield func(e pail.Entry) bool) {
		for e, err := range Entries(ctx, tp.blocks, tp.head) {
			require.NoError(tp.t, err)
			if !yield(e) {
				return
			}
		}
	}
}

func (tp *testPail) Del(ctx context.Context, key string) Result {
	res, err := Del(ctx, tp.blocks, tp.head, key)
	require.NoError(tp.t, err)

	if res.Event != nil {
		err := tp.blocks.Put(ctx, res.Event)
		require.NoError(tp.t, err)
	}

	for _, b := range res.Additions {
		err = tp.blocks.Put(ctx, b)
		require.NoError(tp.t, err)
	}

	tp.head = res.Head
	tp.root = res.Root

	return res
}

func (tp *testPail) Visualize(ctx context.Context) {
	binder := node.BinderFunc[operation.Operation](operation.Bind)
	for line, err := range clock.Visualize(ctx, tp.blocks, binder, tp.head) {
		require.NoError(tp.t, err)
		fmt.Println(line)
	}
}
