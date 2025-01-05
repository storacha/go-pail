package event

import (
	"testing"

	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-pail/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestMarshalUnmarshal(t *testing.T) {
	vectors := []struct {
		Name    string
		Parents []ipld.Link
		Data    string
	}{
		{
			Name: "no parents",
			Data: "test",
		},
		{
			Name: "multi parents",
			Parents: []ipld.Link{
				testutil.RandomLink(t),
				testutil.RandomLink(t),
				testutil.RandomLink(t),
			},
			Data: "test",
		},
	}

	for _, v := range vectors {
		t.Run(v.Name, func(t *testing.T) {
			e := NewEvent(v.Data, v.Parents)
			b, err := MarshalBlock(e, testutil.NewStringBinder(t))
			require.NoError(t, err)

			o, err := Unmarshal(b.Bytes(), testutil.NewStringBinder(t))
			require.NoError(t, err)

			require.Equal(t, v.Parents, o.Parents())
			require.Equal(t, v.Data, o.Data())
		})
	}
}
