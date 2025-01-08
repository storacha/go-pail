package block

import (
	"context"

	"github.com/ipld/go-ipld-prime"
)

// MapBlockstore is a blockstore that is backed by an in memory map.
type MapBlockstore struct {
	data map[ipld.Link]Block
}

func (bs *MapBlockstore) Get(ctx context.Context, link ipld.Link) (Block, error) {
	b, ok := bs.data[link]
	if !ok {
		return nil, ErrNotFound
	}
	return b, nil
}

func (bs *MapBlockstore) Put(ctx context.Context, b Block) error {
	bs.data[b.Link()] = b
	return nil
}

func (bs *MapBlockstore) Del(ctx context.Context, link ipld.Link) error {
	delete(bs.data, link)
	return nil
}

// NewMapBlockstore creates a new blockstore that is backed by an in memory map.
func NewMapBlockstore() *MapBlockstore {
	return &MapBlockstore{map[ipld.Link]Block{}}
}
