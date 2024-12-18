package shard

import (
	"context"

	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-pail/block"
)

type Fetcher struct {
	blocks block.Fetcher
}

func (f *Fetcher) Get(ctx context.Context, link ipld.Link) (BlockView, error) {
	b, err := f.blocks.Get(ctx, link)
	if err != nil {
		return nil, err
	}

	s, err := Decode(b.Bytes())
	if err != nil {
		return nil, err
	}

	return block.NewBlockView(link, b.Bytes(), s), nil
}

func (f *Fetcher) GetRoot(ctx context.Context, link ipld.Link) (RootBlockView, error) {
	b, err := f.blocks.Get(ctx, link)
	if err != nil {
		return nil, err
	}

	rs, err := DecodeRoot(b.Bytes())
	if err != nil {
		return nil, err
	}

	return block.NewBlockView(link, b.Bytes(), rs), nil
}

func NewFetcher(blocks block.Fetcher) *Fetcher {
	return &Fetcher{blocks}
}

func AsBlock[S Shard](b block.BlockView[S]) BlockView {
	return block.NewBlockView(b.Link(), b.Bytes(), Shard(b.Value()))
}
