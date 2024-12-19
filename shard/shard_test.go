package shard_test

import (
	"testing"

	"github.com/storacha/go-pail/internal/testutil"
	"github.com/storacha/go-pail/shard"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecode(t *testing.T) {
	vectors := []struct {
		Name string
		Data shard.Shard
	}{
		{
			Name: "no entries",
			Data: shard.New("", nil),
		},
		{
			Name: "prefix",
			Data: shard.New("foo/bar", nil),
		},
		{
			Name: "value entry",
			Data: shard.New(
				"",
				[]shard.Entry{
					shard.NewEntry("test", shard.NewValue(testutil.RandomLink(t), nil)),
				},
			),
		},
		{
			Name: "shard entry",
			Data: shard.New(
				"",
				[]shard.Entry{
					shard.NewEntry("test", shard.NewValue(nil, testutil.RandomLink(t))),
				},
			),
		},
		{
			Name: "shard and value entry",
			Data: shard.New(
				"",
				[]shard.Entry{
					shard.NewEntry(
						"test",
						shard.NewValue(
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
			b, err := shard.EncodeBlock(v.Data)
			require.NoError(t, err)

			s, err := shard.Decode(b.Bytes())
			require.NoError(t, err)

			require.Equal(t, v.Data, s)
		})
	}
}

func TestEncodeDecodeRoot(t *testing.T) {
	r := shard.NewRoot([]shard.Entry{
		shard.NewEntry("test", shard.NewValue(testutil.RandomLink(t), nil)),
	})
	b, err := shard.Encode(r)
	require.NoError(t, err)

	s, err := shard.DecodeRoot(b)
	require.NoError(t, err)

	require.Equal(t, r, s)
}
