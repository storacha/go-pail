package operation

import (
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

func Unbind(op Operation) (ipld.Node, error) {
	nb := basicnode.Prototype.Any.NewBuilder()
	ma, err := nb.BeginMap(3)
	if err != nil {
		return nil, err
	}

	err = ma.AssembleKey().AssignString("root")
	if err != nil {
		return nil, err
	}
	err = ma.AssembleValue().AssignLink(op.Root())
	if err != nil {
		return nil, err
	}

	err = ma.AssembleKey().AssignString("type")
	if err != nil {
		return nil, err
	}
	err = ma.AssembleValue().AssignString(op.Type())
	if err != nil {
		return nil, err
	}

	err = ma.AssembleKey().AssignString("key")
	if err != nil {
		return nil, err
	}
	err = ma.AssembleValue().AssignString(op.Key())
	if err != nil {
		return nil, err
	}

	if op.Type() == TypePut {
		err = ma.AssembleKey().AssignString("value")
		if err != nil {
			return nil, err
		}
		err = ma.AssembleValue().AssignLink(op.Value())
		if err != nil {
			return nil, err
		}
	}

	err = ma.Finish()
	if err != nil {
		return nil, err
	}

	return nb.Build(), nil
}

func Bind(n ipld.Node) (Operation, error) {
	op := operation{}

	rn, err := n.LookupByString("root")
	if err != nil {
		return nil, err
	}
	r, err := rn.AsLink()
	if err != nil {
		return nil, err
	}
	op.root = r

	tn, err := n.LookupByString("type")
	if err != nil {
		return nil, err
	}
	t, err := tn.AsString()
	if err != nil {
		return nil, err
	}
	op.typ = t

	kn, err := n.LookupByString("key")
	if err != nil {
		return nil, err
	}
	k, err := kn.AsString()
	if err != nil {
		return nil, err
	}
	op.key = k

	if op.typ == TypePut {
		vn, err := n.LookupByString("value")
		if err != nil {
			return nil, err
		}
		v, err := vn.AsLink()
		if err != nil {
			return nil, err
		}
		op.val = v
	}

	return op, nil
}
