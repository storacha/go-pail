package block

import (
	"context"

	"github.com/ipld/go-ipld-prime"
)

type Block interface {
	Link() ipld.Link
	Bytes() []byte
}

type BlockView[T any] interface {
	Block
	Value() T
}

type Fetcher interface {
	Get(ctx context.Context, link ipld.Link) (Block, error)
}
