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

type MemoryBlockstore struct {
	data map[string]block.Block
}

func (bs *MemoryBlockstore) Get(ctx context.Context, link ipld.Link) (block.Block, error) {
	b, ok := bs.data[link.String()]
	if !ok {
		return nil, ErrNotFound
	}
	return b, nil
}

func (bs *MemoryBlockstore) Put(ctx context.Context, b block.Block) error {
	bs.data[b.Link().String()] = b
	return nil
}

func (bs *MemoryBlockstore) Del(ctx context.Context, link ipld.Link) error {
	delete(bs.data, link.String())
	return nil
}

func NewBlockstore() *MemoryBlockstore {
	return &MemoryBlockstore{map[string]block.Block{}}
}
