package pail

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-pail/block"
	"github.com/storacha/go-pail/shard"
)

// Put a value (a CID) for the given key. If the key exists it's value is
// overwritten.
func Put(ctx context.Context, blocks block.Fetcher, root ipld.Link, key string, value ipld.Link) (ipld.Link, shard.Diff, error) {
	shards := shard.NewFetcher(blocks)
	rshard, err := shards.GetRoot(ctx, root)
	if err != nil {
		return nil, shard.Diff{}, err
	}

	if rshard.Value().KeyChars() != shard.KeyCharsASCII {
		return nil, shard.Diff{}, fmt.Errorf("unsupported key character set: %s", rshard.Value().KeyChars())
	}
	if !shard.IsPrintableASCII(key) {
		return nil, shard.Diff{}, errors.New("key contains non-ASCII characters")
	}
	if int64(len(key)) > rshard.Value().MaxKeySize() {
		return nil, shard.Diff{}, fmt.Errorf("UTF-8 encoded key exceeds max size of %d bytes", rshard.Value().MaxKeySize())
	}

	path, err := traverse(ctx, shards, shard.AsBlock(rshard), key)
	if err != nil {
		return nil, shard.Diff{}, fmt.Errorf("traversing shard: %w", err)
	}
	target := path[len(path)-1]
	skey := key[len(target.Value().Prefix()):]

	entry := shard.NewEntry(skey, shard.NewValue(value, nil))
	targetEntries := target.Value().Entries()[:]

	var additions []shard.BlockView
	for i, e := range targetEntries {
		k := e.Key()
		v := e.Value()

		// is this just a replace?
		if k == skey {
			break
		}

		// do we need to shard this entry?
		var shortest string
		var longest string
		if len(k) < len(skey) {
			shortest = k
			longest = skey
		} else {
			shortest = skey
			longest = k
		}

		common := ""
		for _, char := range shortest {
			next := common + string(char)
			if !strings.HasPrefix(longest, next) {
				break
			}
			common = next
		}
		if common != "" {
			var entries []shard.Entry

			// if the existing entry key or new key is equal to the common prefix,
			// then the existing value / new value needs to persist in the parent
			// shard. Otherwise they persist in this new shard.
			if common != skey {
				entries = shard.PutEntry(
					entries,
					shard.NewEntry(skey[len(common):], shard.NewValue(value, nil)),
				)
			}
			if common != k {
				entries = shard.PutEntry(entries, shard.NewEntry(k[len(common):], v))
			}

			child, err := shard.EncodeBlock(shard.New(target.Value().Prefix()+common, entries))
			if err != nil {
				return nil, shard.Diff{}, err
			}
			additions = append(additions, child)

			// create parent shards for each character of the common prefix
			var commonChars []string
			for _, c := range common {
				commonChars = append(commonChars, string(c))
			}
			for i := len(commonChars) - 1; i > 0; i-- {
				parentPrefix := target.Value().Prefix() + strings.Join(commonChars[0:i], "")
				var parentValue shard.Value

				// if the first iteration and the existing entry key is equal to the
				// common prefix, then existing value needs to persist in this parent
				if i == len(commonChars)-1 && common == k {
					if v.Shard() != nil {
						return nil, shard.Diff{}, errors.New("found a shard link when expecting a value")
					}
					parentValue = shard.NewValue(v.Value(), child.Link())
				} else if i == len(commonChars)-1 && common == skey {
					parentValue = shard.NewValue(value, child.Link())
				} else {
					parentValue = shard.NewValue(nil, child.Link())
				}

				parent, err := shard.EncodeBlock(
					shard.New(
						parentPrefix,
						[]shard.Entry{shard.NewEntry(commonChars[i], parentValue)},
					),
				)
				if err != nil {
					return nil, shard.Diff{}, err
				}
				additions = append(additions, parent)
				child = parent
			}

			// remove the sharded entry
			targetEntries = slices.Delete(targetEntries, i, i+1)

			// create the entry that will be added to target
			if len(commonChars) == 1 && common == k {
				entry = shard.NewEntry(commonChars[0], shard.NewValue(v.Value(), child.Link()))
			} else if len(commonChars) == 1 && common == skey {
				entry = shard.NewEntry(commonChars[0], shard.NewValue(value, child.Link()))
			} else {
				entry = shard.NewEntry(commonChars[0], shard.NewValue(nil, child.Link()))
			}
			break
		}
	}

	var nshard shard.Shard
	if target.Value().Prefix() == "" {
		nshard = shard.NewRoot(shard.PutEntry(targetEntries, entry))
	} else {
		nshard = shard.New(target.Value().Prefix(), shard.PutEntry(targetEntries, entry))
	}

	child, err := shard.EncodeBlock(nshard)
	if err != nil {
		return nil, shard.Diff{}, err
	}

	// if no change in the target then we're done
	if child.Link().String() == target.Link().String() {
		return root, shard.Diff{}, nil
	}

	additions = append(additions, child)

	// path is root -> target, so work backwards, propagating the new shard CID
	for i := len(path) - 2; i >= 0; i-- {
		parent := path[i]
		key := child.Value().Prefix()[len(parent.Value().Prefix()):]

		entries := parent.Value().Entries()[:]
		for i, e := range entries {
			if e.Key() == key {
				if e.Value().Shard() == nil {
					return nil, shard.Diff{}, fmt.Errorf("\"%s\" is not a shard link in: %s", key, parent.Link().String())
				}
				entries[i] = shard.NewEntry(key, shard.NewValue(e.Value().Value(), child.Link()))
				break
			}
		}

		var cshard shard.Shard
		if parent.Value().Prefix() == "" {
			cshard = shard.NewRoot(entries)
		} else {
			cshard = shard.New(parent.Value().Prefix(), entries)
		}

		child, err = shard.EncodeBlock(cshard)
		if err != nil {
			return nil, shard.Diff{}, err
		}
		additions = append(additions, child)
	}

	return additions[len(additions)-1].Link(), shard.Diff{Additions: additions, Removals: path}, nil
}
