package shard

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-pail/block"
)

// Entry is a single key/value entry within a shard.
type Entry interface {
	Key() string
	Value() Value
}

type entry struct {
	key   string
	value Value
}

func (e entry) Key() string {
	return e.key
}

func (e entry) Value() Value {
	return e.value
}

func NewEntry(k string, v Value) Entry {
	return entry{k, v}
}

type Value interface {
	// Shard is a link to a shard, which may be nil if this value is a link to
	// user data.
	Shard() ipld.Link
	// Value is a link to user data, which may be nil if this value is a link to
	// a shard.
	Value() ipld.Link
}

type value struct {
	shard ipld.Link
	value ipld.Link
}

func (v value) Shard() ipld.Link {
	return v.shard
}

func (v value) Value() ipld.Link {
	return v.value
}

func NewValue(val ipld.Link, shard ipld.Link) Value {
	return value{shard, val}
}

type Shard interface {
	// Prefix is the key prefix from the root to this shard.
	Prefix() string
	Entries() []Entry
}

type shard struct {
	prefix  string
	entries []Entry
}

func (s shard) Prefix() string {
	return s.prefix
}

func (s shard) Entries() []Entry {
	return s.entries
}

func New(prefix string, entries []Entry) Shard {
	return shard{prefix, entries}
}

type RootShard interface {
	Shard
	// Version is the shard compatibility version.
	Version() int64
	// KeyChars are the characters allowed in keys, referring to a known character
	// set. e.g. "ascii" refers to the printable ASCII characters in the code
	// range 32-126.
	KeyChars() string
	// MaxKeySize is the maximum key size in bytes - default 4096 bytes.
	MaxKeySize() int64
}

type rootshard struct {
	shard
	version    int64
	keyChars   string
	maxKeySize int64
}

func (r rootshard) KeyChars() string {
	return r.keyChars
}

func (r rootshard) MaxKeySize() int64 {
	return r.maxKeySize
}

func (r rootshard) Version() int64 {
	return r.version
}

func NewRoot(entries []Entry) RootShard {
	return rootshard{shard: shard{"", entries}, version: Version, keyChars: KeyCharsASCII, maxKeySize: MaxKeySize}
}

const Version = 2

// Marshal serializes a [Shard] or a [RootShard] to CBOR encoded bytes.
func Marshal(s Shard) ([]byte, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()

	ma, err := nb.BeginMap(5)
	if err != nil {
		return nil, fmt.Errorf("beginning map: %w", err)
	}

	if rs, ok := s.(RootShard); ok {
		err = ma.AssembleKey().AssignString("version")
		if err != nil {
			return nil, fmt.Errorf("assembling version key: %w", err)
		}
		err = ma.AssembleValue().AssignInt(rs.Version())
		if err != nil {
			return nil, fmt.Errorf("assembling version value: %w", err)
		}

		err = ma.AssembleKey().AssignString("keyChars")
		if err != nil {
			return nil, fmt.Errorf("assembling key characters key: %w", err)
		}
		err = ma.AssembleValue().AssignString(rs.KeyChars())
		if err != nil {
			return nil, fmt.Errorf("assembling key characters value: %w", err)
		}

		err = ma.AssembleKey().AssignString("maxKeySize")
		if err != nil {
			return nil, fmt.Errorf("assembling maximum key size key: %w", err)
		}
		err = ma.AssembleValue().AssignInt(rs.MaxKeySize())
		if err != nil {
			return nil, fmt.Errorf("assembling maximum key size value: %w", err)
		}
	}

	err = ma.AssembleKey().AssignString("prefix")
	if err != nil {
		return nil, fmt.Errorf("assembling prefix key: %w", err)
	}
	err = ma.AssembleValue().AssignString(s.Prefix())
	if err != nil {
		return nil, fmt.Errorf("assembling prefix value: %w", err)
	}

	err = ma.AssembleKey().AssignString("entries")
	if err != nil {
		return nil, fmt.Errorf("assembling entries key: %w", err)
	}

	enb := np.NewBuilder()
	la, err := enb.BeginList(int64(len(s.Entries())))
	if err != nil {
		return nil, fmt.Errorf("beginning entries list: %w", err)
	}

	for _, ent := range s.Entries() {
		n, err := unwrapEntry(ent)
		if err != nil {
			return nil, fmt.Errorf("encoding entry node: %w", err)
		}
		err = la.AssembleValue().AssignNode(n)
		if err != nil {
			return nil, fmt.Errorf("assembling entry value: %w", err)
		}
	}

	err = la.Finish()
	if err != nil {
		return nil, fmt.Errorf("finishing entries list: %w", err)
	}

	err = ma.AssembleValue().AssignNode(enb.Build())
	if err != nil {
		return nil, fmt.Errorf("assembling entries value: %w", err)
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

func unwrapEntry(e Entry) (datamodel.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	la, err := nb.BeginList(2)
	if err != nil {
		return nil, fmt.Errorf("beginning entry list: %w", err)
	}
	err = la.AssembleValue().AssignString(e.Key())
	if err != nil {
		return nil, fmt.Errorf("assembling entry key: %w", err)
	}
	vn, err := unwrapValue(e.Value())
	if err != nil {
		return nil, fmt.Errorf("encoding entry value: %w", err)
	}
	err = la.AssembleValue().AssignNode(vn)
	if err != nil {
		return nil, fmt.Errorf("assembling entry value: %w", err)
	}
	err = la.Finish()
	if err != nil {
		return nil, fmt.Errorf("finishing entry list: %w", err)
	}
	return nb.Build(), nil
}

func unwrapValue(v Value) (datamodel.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()

	if v.Shard() == nil {
		err := nb.AssignLink(v.Value())
		if err != nil {
			return nil, fmt.Errorf("assembling value link: %w", err)
		}
		return nb.Build(), nil
	}

	la, err := nb.BeginList(1)
	if err != nil {
		return nil, fmt.Errorf("beginning value list: %w", err)
	}
	err = la.AssembleValue().AssignLink(v.Shard())
	if err != nil {
		return nil, fmt.Errorf("assembling shard link: %w", err)
	}
	if v.Value() != nil {
		err = la.AssembleValue().AssignLink(v.Value())
		if err != nil {
			return nil, fmt.Errorf("assembling value link: %w", err)
		}
	}
	err = la.Finish()
	if err != nil {
		return nil, fmt.Errorf("finishing value list: %w", err)
	}
	return nb.Build(), nil
}

// MarshalBlock serializes the [Shard] to CBOR encoded bytes, takes the sha2-256
// hash of the data, constructs a CID and returns a [block.Block].
func MarshalBlock[S Shard](s S) (block.BlockView[S], error) {
	bytes, err := Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("marshalling shard: %w", err)
	}
	digest, err := multihash.Sum(bytes, multihash.SHA2_256, -1)
	if err != nil {
		return nil, fmt.Errorf("sha256 hashing: %w", err)
	}
	link := cidlink.Link{Cid: cid.NewCidV1(uint64(multicodec.DagCbor), digest)}
	return block.NewBlockView(link, bytes, s), nil
}

// Unmarshal deserializes CBOR encoded bytes to a [Shard].
func Unmarshal(b []byte) (Shard, error) {
	var s shard

	np := basicnode.Prototype.Map
	nb := np.NewBuilder()
	err := dagcbor.Decode(nb, bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("decoding shard: %w", err)
	}
	n := nb.Build()

	pfxn, err := n.LookupByString("prefix")
	if err != nil {
		return nil, fmt.Errorf("looking up prefix: %w", err)
	}
	prefix, err := pfxn.AsString()
	if err != nil {
		return nil, fmt.Errorf("decoding prefix as string: %w", err)
	}
	s.prefix = prefix

	en, err := n.LookupByString("entries")
	if err != nil {
		return nil, fmt.Errorf("looking up entries: %w", err)
	}
	ents := en.ListIterator()
	if ents == nil {
		return nil, errors.New("entries is not a list")
	}
	for {
		if ents.Done() {
			break
		}
		_, n, err := ents.Next()
		if err != nil {
			return nil, fmt.Errorf("iterating entries: %w", err)
		}
		ent, err := wrapEntry(n)
		if err != nil {
			return nil, fmt.Errorf("decoding entry node: %w", err)
		}
		s.entries = append(s.entries, ent)
	}

	return s, nil
}

// UnmarshalRoot deserializes CBOR encoded bytes to a [RootShard].
func UnmarshalRoot(b []byte) (RootShard, error) {
	var rs rootshard

	np := basicnode.Prototype.Map
	nb := np.NewBuilder()
	err := dagcbor.Decode(nb, bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("CBOR decoding: %w", err)
	}
	n := nb.Build()

	vn, err := n.LookupByString("version")
	if err != nil {
		return nil, fmt.Errorf("looking up version: %w", err)
	}
	version, err := vn.AsInt()
	if err != nil {
		return nil, fmt.Errorf("decoding version as int: %w", err)
	}
	if version != Version {
		return nil, fmt.Errorf("unsupported version: %d", version)
	}
	rs.version = version

	kcn, err := n.LookupByString("keyChars")
	if err != nil {
		return nil, fmt.Errorf("looking up key characters: %w", err)
	}
	keyChars, err := kcn.AsString()
	if err != nil {
		return nil, fmt.Errorf("decoding key characters as string: %w", err)
	}
	rs.keyChars = keyChars

	mksn, err := n.LookupByString("maxKeySize")
	if err != nil {
		return nil, fmt.Errorf("looking up maximum key size: %w", err)
	}
	maxKeySize, err := mksn.AsInt()
	if err != nil {
		return nil, fmt.Errorf("decoding maximum key size as int: %w", err)
	}
	rs.maxKeySize = maxKeySize

	pfxn, err := n.LookupByString("prefix")
	if err != nil {
		return nil, fmt.Errorf("looking up prefix: %w", err)
	}
	prefix, err := pfxn.AsString()
	if err != nil {
		return nil, fmt.Errorf("decoding prefix as string: %w", err)
	}
	rs.prefix = prefix

	en, err := n.LookupByString("entries")
	if err != nil {
		return nil, fmt.Errorf("looking up entries: %w", err)
	}
	ents := en.ListIterator()
	if ents == nil {
		return nil, errors.New("entries is not a list")
	}
	for {
		if ents.Done() {
			break
		}
		_, n, err := ents.Next()
		if err != nil {
			return nil, fmt.Errorf("iterating entries: %w", err)
		}
		ent, err := wrapEntry(n)
		if err != nil {
			return nil, fmt.Errorf("decoding entry node: %w", err)
		}
		rs.entries = append(rs.entries, ent)
	}

	return rs, nil
}

func wrapEntry(n datamodel.Node) (Entry, error) {
	kn, err := n.LookupByIndex(0)
	if err != nil {
		return nil, fmt.Errorf("looking up key: %w", err)
	}
	key, err := kn.AsString()
	if err != nil {
		return nil, fmt.Errorf("decoding key: %w", err)
	}
	vn, err := n.LookupByIndex(1)
	if err != nil {
		return nil, fmt.Errorf("looking up value: %w", err)
	}
	value, err := wrapValue(vn)
	if err != nil {
		return nil, fmt.Errorf("decoding value node: %w", err)
	}
	return entry{key, value}, nil
}

func wrapValue(n datamodel.Node) (Value, error) {
	l, err := n.AsLink()
	if err == nil {
		return value{nil, l}, nil
	}
	sn, err := n.LookupByIndex(0)
	if err != nil {
		return nil, fmt.Errorf("looking up shard link: %w", err)
	}
	shard, err := sn.AsLink()
	if err != nil {
		return nil, fmt.Errorf("decoding shard link: %w", err)
	}
	vn, err := n.LookupByIndex(1)
	// TODO: distinguish between out of range vs other
	if err != nil {
		return value{shard, nil}, nil
	}
	val, err := vn.AsLink()
	if err != nil {
		return nil, fmt.Errorf("decoding value link: %w", err)
	}
	return value{shard, val}, nil
}

func PutEntry(target []Entry, newEntry Entry) []Entry {
	var entries []Entry

	for i, entry := range target {
		k := entry.Key()
		v := entry.Value()

		if newEntry.Key() == k {
			// if new value is link to shard...
			if newEntry.Value().Shard() != nil {
				// and old value is link to shard
				// and old value is _also_ link to data
				// and new value does not have link to data
				// then preserve old data
				if v.Shard() != nil && v.Value() != nil && newEntry.Value().Value() == nil {
					entries = append(entries, NewEntry(k, NewValue(v.Value(), newEntry.Value().Shard())))
				} else {
					entries = append(entries, newEntry)
				}
			} else {
				// shard as well as value?
				if v.Shard() != nil {
					entries = append(entries, NewEntry(k, NewValue(newEntry.Value().Value(), v.Shard())))
				} else {
					entries = append(entries, newEntry)
				}
			}
			return append(entries, target[i+1:]...)
		}

		if i == 0 && newEntry.Key() < k {
			entries = append(entries, newEntry)
			return append(entries, target[i:]...)
		}

		if i > 0 && newEntry.Key() > target[i-1].Key() && newEntry.Key() < k {
			entries = append(entries, newEntry)
			return append(entries, target[i:]...)
		}

		entries = append(entries, entry)
	}

	return append(entries, newEntry)
}

var asciiRegex, _ = regexp.Compile("^[\x20-\x7E]*$")

func IsPrintableASCII(s string) bool {
	return asciiRegex.MatchString(s)
}
