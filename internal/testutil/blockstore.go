package testutil

import (
	"context"
	"errors"

	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-pail/block"
)

var ErrNotFound = errors.New("not found")

type Blockstore interface {
	block.Fetcher
	Put(ctx context.Context, b block.Block) error
	Del(ctx context.Context, link ipld.Link) error
}

type MapBlockstore struct {
	data     map[string]block.Block
	GetCount int
}

func (bs *MapBlockstore) Get(ctx context.Context, link ipld.Link) (block.Block, error) {
	b, ok := bs.data[link.String()]
	if !ok {
		return nil, ErrNotFound
	}
	bs.GetCount++
	return b, nil
}

func (bs *MapBlockstore) Put(ctx context.Context, b block.Block) error {
	bs.data[b.Link().String()] = b
	return nil
}

func (bs *MapBlockstore) PutAll(ctx context.Context, blocks ...block.Block) error {
	for _, b := range blocks {
		bs.Put(ctx, b)
	}
	return nil
}

func (bs *MapBlockstore) Del(ctx context.Context, link ipld.Link) error {
	delete(bs.data, link.String())
	return nil
}

func NewBlockstore() *MapBlockstore {
	return &MapBlockstore{map[string]block.Block{}, 0}
}
