package operation

import "github.com/ipld/go-ipld-prime"

const (
	TypePut = "put"
	TypeDel = "del"
)

type operation struct {
	root ipld.Link
	typ  string
	key  string
	val  ipld.Link
}

func (op operation) Root() ipld.Link {
	return op.root
}

func (op operation) Type() string {
	return op.typ
}

func (op operation) Key() string {
	return op.key
}

func (op operation) Value() ipld.Link {
	return op.val
}

func NewPut(root ipld.Link, key string, value ipld.Link) Operation {
	return operation{root, TypePut, key, value}
}

func NewDel(root ipld.Link, key string) Operation {
	return operation{root, TypeDel, key, nil}
}
