package crdt

import (
	"context"
	"fmt"
	"testing"

	"github.com/ipld/go-ipld-prime"
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

	testutil.ApplyDiff(tp.t, diff, tp.blocks)
	tp.head = head
	tp.root = root

	return head
}

func (tp *testPail) Put(ctx context.Context, key string, value ipld.Link) Result {
	result, err := Put(ctx, tp.blocks, tp.head, key, value)
	require.NoError(tp.t, err)

	if result.Event != nil {
		err := tp.blocks.Put(ctx, result.Event)
		require.NoError(tp.t, err)
	}

	testutil.ApplyDiff(tp.t, result.Diff, tp.blocks)
	tp.head = result.Head
	tp.root = result.Root

	return result
}

func (tp *testPail) Visualize(ctx context.Context) {
	binder := node.BinderFunc[operation.Operation](operation.Bind)
	for line, err := range clock.Visualize(ctx, tp.blocks, binder, tp.head) {
		require.NoError(tp.t, err)
		fmt.Println(line)
	}
}
