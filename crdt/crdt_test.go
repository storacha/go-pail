package crdt

import (
	"context"
	"testing"

	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-pail/clock"
	"github.com/storacha/go-pail/crdt/operation"
	"github.com/storacha/go-pail/internal/testutil"
	"github.com/storacha/go-pail/ipld/node"
	"github.com/stretchr/testify/require"
)

func TestCRDT(t *testing.T) {
	t.Run("put a value to a new clock", func(t *testing.T) {
		bs := testutil.NewBlockstore()
		alice := testPail{t: t, blocks: bs}
	})
}

type testPail struct {
	t      *testing.T
	blocks *testutil.MapBlockstore
	head   []ipld.Link
	root   ipld.Link
}

func (p *testPail) Advance(ctx context.Context, event ipld.Link) {
	head, err := clock.Advance(ctx, p.blocks, node.BinderFunc[operation.Operation](operation.Bind), p.head, event)
	require.NoError(p.t, err)
	p.head = head

	result, err := Root(p.blocks, head)
	require.NoError(p.t, err)
}
