package event

import (
	"context"

	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-pail/block"
	"github.com/storacha/go-pail/ipld/node"
)

type Fetcher[T any] struct {
	blocks     block.Fetcher
	dataBinder node.Binder[T]
}

func (f *Fetcher[T]) Get(ctx context.Context, link ipld.Link) (BlockView[T], error) {
	b, err := f.blocks.Get(ctx, link)
	if err != nil {
		return nil, err
	}

	s, err := Unmarshal(b.Bytes(), f.dataBinder)
	if err != nil {
		return nil, err
	}

	return block.NewBlockView(link, b.Bytes(), s), nil
}

func NewFetcher[T any](blocks block.Fetcher, dataBinder node.Binder[T]) *Fetcher[T] {
	return &Fetcher[T]{blocks, dataBinder}
}
