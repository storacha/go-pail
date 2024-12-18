package shard

import (
	"testing"

	"github.com/storacha/go-pail/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecode(t *testing.T) {
	vectors := []struct {
		Name string
		Data Shard
	}{
		{
			Name: "no entries",
			Data: New("", nil),
		},
		{
			Name: "prefix",
			Data: New("foo/bar", nil),
		},
		{
			Name: "value entry",
			Data: New(
				"",
				[]Entry{
					NewEntry("test", NewValue(testutil.RandomLink(t), nil)),
				},
			),
		},
		{
			Name: "shard entry",
			Data: New(
				"",
				[]Entry{
					NewEntry("test", NewValue(nil, testutil.RandomLink(t))),
				},
			),
		},
		{
			Name: "shard and value entry",
			Data: New(
				"",
				[]Entry{
					NewEntry(
						"test",
						NewValue(
							testutil.RandomLink(t),
							testutil.RandomLink(t),
						),
					),
				},
			),
		},
	}

	for _, v := range vectors {
		t.Run(v.Name, func(t *testing.T) {
			b, err := Encode(v.Data)
			require.NoError(t, err)

			s, err := Decode(b)
			require.NoError(t, err)

			require.Equal(t, v.Data, s)
		})
	}
}
