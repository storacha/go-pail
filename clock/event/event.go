package event

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-pail/block"
	"github.com/storacha/go-pail/ipld/multicodec"
	"github.com/storacha/go-pail/ipld/node"
)

type event[T any] struct {
	parents []ipld.Link
	data    T
}

func (e event[T]) Parents() []ipld.Link {
	return e.parents
}

func (e event[T]) Data() T {
	return e.data
}

func NewEvent[T any](data T, parents []ipld.Link) Event[T] {
	return event[T]{parents, data}
}

// Unmarshal deserializes CBOR encoded bytes to an [Event].
func Unmarshal[T any](b []byte, dataBinder node.Binder[T]) (Event[T], error) {
	var e event[T]

	np := basicnode.Prototype.Map
	nb := np.NewBuilder()
	err := dagcbor.Decode(nb, bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("decoding event: %w", err)
	}
	n := nb.Build()

	pn, err := n.LookupByString("parents")
	if err != nil {
		return nil, fmt.Errorf("looking up parents: %w", err)
	}

	parents := pn.ListIterator()
	if parents == nil {
		return nil, errors.New("parents is not a list")
	}
	for {
		if parents.Done() {
			break
		}
		_, n, err := parents.Next()
		if err != nil {
			return nil, fmt.Errorf("iterating parents: %w", err)
		}
		p, err := n.AsLink()
		if err != nil {
			return nil, fmt.Errorf("decoding parent as link: %w", err)
		}
		e.parents = append(e.parents, p)
	}

	dn, err := n.LookupByString("data")
	if err != nil {
		return nil, fmt.Errorf("looking up data: %w", err)
	}

	data, err := dataBinder.Bind(dn)
	if err != nil {
		return nil, fmt.Errorf("binding data: %w", err)
	}

	e.data = data
	return e, nil
}

// Marshal serializes an [Event] to CBOR encoded bytes.
func Marshal[T any](event Event[T], dataUnbinder node.Unbinder[T]) ([]byte, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()

	ma, err := nb.BeginMap(2)
	if err != nil {
		return nil, fmt.Errorf("beginning map: %w", err)
	}

	err = ma.AssembleKey().AssignString("parents")
	if err != nil {
		return nil, fmt.Errorf("assembling parents key: %w", err)
	}

	pnb := np.NewBuilder()
	la, err := pnb.BeginList(int64(len(event.Parents())))
	if err != nil {
		return nil, fmt.Errorf("beginning parents list: %w", err)
	}

	for _, link := range event.Parents() {
		err = la.AssembleValue().AssignLink(link)
		if err != nil {
			return nil, fmt.Errorf("assembling link value: %w", err)
		}
	}

	err = la.Finish()
	if err != nil {
		return nil, fmt.Errorf("finishing parents list: %w", err)
	}

	err = ma.AssembleValue().AssignNode(pnb.Build())
	if err != nil {
		return nil, fmt.Errorf("assembling parents value: %w", err)
	}

	err = ma.AssembleKey().AssignString("data")
	if err != nil {
		return nil, fmt.Errorf("assembling data key: %w", err)
	}

	dnd, err := dataUnbinder.Unbind(event.Data())
	if err != nil {
		return nil, err
	}

	err = ma.AssembleValue().AssignNode(dnd)
	if err != nil {
		return nil, fmt.Errorf("assembling parents value: %w", err)
	}

	err = ma.Finish()
	if err != nil {
		return nil, fmt.Errorf("finishing map: %w", err)
	}

	n := nb.Build()
	buf := bytes.NewBuffer([]byte{})
	err = dagcbor.Encode(n, buf)
	if err != nil {
		return nil, fmt.Errorf("CBOR encoding: %w", err)
	}
	return buf.Bytes(), nil
}

// MarshalBlock serializes the [Event] to CBOR encoded bytes, takes the sha2-256
// hash of the data, constructs a CID and returns a [block.Block].
func MarshalBlock[T any](e Event[T], dataUnbinder node.Unbinder[T]) (block.BlockView[Event[T]], error) {
	bytes, err := Marshal(e, dataUnbinder)
	if err != nil {
		return nil, fmt.Errorf("marshalling event: %w", err)
	}
	digest, err := multihash.Sum(bytes, multihash.SHA2_256, -1)
	if err != nil {
		return nil, fmt.Errorf("sha256 hashing: %w", err)
	}
	link := cidlink.Link{Cid: cid.NewCidV1(uint64(multicodec.DagCbor), digest)}
	return block.NewBlockView(link, bytes, e), nil
}
