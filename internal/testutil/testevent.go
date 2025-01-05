package testutil

import (
	"testing"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/stretchr/testify/require"
)

type TestEvent struct {
	Operation string
	Key       string
	Value     ipld.Link
}

func RandomEventData(t *testing.T) TestEvent {
	return TestEvent{"put", RandomLink(t).String(), RandomLink(t)}
}

type TestEventBinder struct {
	t *testing.T
}

func (teb TestEventBinder) Unbind(e TestEvent) (ipld.Node, error) {
	nb := basicnode.Prototype.Any.NewBuilder()
	ma, err := nb.BeginMap(3)
	require.NoError(teb.t, err)

	err = ma.AssembleKey().AssignString("operation")
	require.NoError(teb.t, err)

	err = ma.AssembleValue().AssignString(e.Operation)
	require.NoError(teb.t, err)

	err = ma.AssembleKey().AssignString("key")
	require.NoError(teb.t, err)

	err = ma.AssembleValue().AssignString(e.Key)
	require.NoError(teb.t, err)

	err = ma.AssembleKey().AssignString("value")
	require.NoError(teb.t, err)

	err = ma.AssembleValue().AssignLink(e.Value)
	require.NoError(teb.t, err)

	err = ma.Finish()
	require.NoError(teb.t, err)

	return nb.Build(), nil
}

func (teb TestEventBinder) Bind(n ipld.Node) (TestEvent, error) {
	e := TestEvent{}

	on, err := n.LookupByString("operation")
	require.NoError(teb.t, err)

	op, err := on.AsString()
	require.NoError(teb.t, err)
	e.Operation = op

	kn, err := n.LookupByString("key")
	require.NoError(teb.t, err)

	key, err := kn.AsString()
	require.NoError(teb.t, err)
	e.Key = key

	vn, err := n.LookupByString("value")
	require.NoError(teb.t, err)

	value, err := vn.AsLink()
	require.NoError(teb.t, err)
	e.Value = value

	return e, nil
}

func NewTestEventBinder(t *testing.T) TestEventBinder {
	return TestEventBinder{t}
}
