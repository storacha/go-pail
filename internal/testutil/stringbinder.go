package testutil

import (
	"testing"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/stretchr/testify/require"
)

type StringBinder struct {
	t *testing.T
}

func (sb StringBinder) Unbind(s string) (ipld.Node, error) {
	nb := basicnode.Prototype.String.NewBuilder()
	err := nb.AssignString(s)
	require.NoError(sb.t, err)
	return nb.Build(), nil
}

func (sb StringBinder) Bind(n ipld.Node) (string, error) {
	s, err := n.AsString()
	require.NoError(sb.t, err)
	return s, nil
}

func NewStringBinder(t *testing.T) StringBinder {
	return StringBinder{t}
}
