package block

import (
	"context"

	"github.com/ipld/go-ipld-prime"
)

// MapBlockstore is a blockstore that is backed by an in memory map.
type MapBlockstore struct {
	data map[string]Block
}

func (bs *MapBlockstore) Get(ctx context.Context, link ipld.Link) (Block, error) {
	b, ok := bs.data[link.String()]
	if !ok {
		return nil, ErrNotFound
	}
	return b, nil
}

func (bs *MapBlockstore) Put(ctx context.Context, b Block) error {
	bs.data[b.Link().String()] = b
	return nil
}

func (bs *MapBlockstore) Del(ctx context.Context, link ipld.Link) error {
	delete(bs.data, link.String())
	return nil
}

// NewMapBlockstore creates a new blockstore that is backed by an in memory map.
func NewMapBlockstore() *MapBlockstore {
	return &MapBlockstore{map[string]Block{}}
}
