package event

import (
	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-pail/block"
)

type Event[T any] interface {
	Parents() []ipld.Link
	Data() T
}

type BlockView[T any] interface {
	block.BlockView[Event[T]]
}

type NodeUnbinder[T any] interface {
	Unbind(T) (ipld.Node, error)
}

type NodeBinder[T any] interface {
	Bind(ipld.Node) (T, error)
}
