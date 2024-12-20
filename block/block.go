package block

import (
	"errors"

	"github.com/ipld/go-ipld-prime"
)

var ErrNotFound = errors.New("not found")

type block struct {
	link  ipld.Link
	bytes []byte
}

func (b block) Link() ipld.Link {
	return b.link
}

func (b block) Bytes() []byte {
	return b.bytes
}

func New(link ipld.Link, bytes []byte) Block {
	return block{link, bytes}
}

type blockView[T any] struct {
	block
	value T
}

func (v blockView[T]) Value() T {
	return v.value
}

func NewBlockView[T any](link ipld.Link, bytes []byte, value T) BlockView[T] {
	return blockView[T]{block{link, bytes}, value}
}
