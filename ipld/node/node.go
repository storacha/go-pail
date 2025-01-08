package node

import "github.com/ipld/go-ipld-prime"

type Unbinder[T any] interface {
	Unbind(T) (ipld.Node, error)
}

type UnbinderFunc[T any] func(T) (ipld.Node, error)

func (f UnbinderFunc[T]) Unbind(t T) (ipld.Node, error) {
	return f(t)
}

type Binder[T any] interface {
	Bind(ipld.Node) (T, error)
}

type BinderFunc[T any] func(ipld.Node) (T, error)

func (f BinderFunc[T]) Bind(n ipld.Node) (T, error) {
	return f(n)
}
