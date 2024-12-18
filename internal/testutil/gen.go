package testutil

import (
	crand "crypto/rand"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func RandomBytes(t *testing.T, size int) []byte {
	t.Helper()
	bytes := make([]byte, size)
	_, err := crand.Read(bytes)
	require.NoError(t, err)
	return bytes
}

func RandomLink(t *testing.T) datamodel.Link {
	bytes := RandomBytes(t, 10)
	c, _ := cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   mh.SHA2_256,
		MhLength: -1,
	}.Sum(bytes)
	return cidlink.Link{Cid: c}
}

func RandomMultihash(t *testing.T) mh.Multihash {
	return RandomLink(t).(cidlink.Link).Hash()
}
