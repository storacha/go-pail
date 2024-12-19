package pail

import (
	"context"
	"fmt"
	"iter"
	"strings"

	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-pail/block"
	"github.com/storacha/go-pail/shard"
)

type EntriesOption func(*entriesOptions)

type entriesOptions struct {
	prefix string
	gt     string
	gte    string
	lt     string
	lte    string
}

func WithKeyPrefix(prefix string) EntriesOption {
	return func(o *entriesOptions) {
		o.prefix = prefix
	}
}

func WithKeyGreaterThan(gt string) EntriesOption {
	return func(o *entriesOptions) {
		o.gt = gt
	}
}

func WithKeyGreaterThanOrEqual(gte string) EntriesOption {
	return func(o *entriesOptions) {
		o.gte = gte
	}
}

func WithKeyLessThan(lt string) EntriesOption {
	return func(o *entriesOptions) {
		o.lt = lt
	}
}

func WithKeyLessThanOrEqual(lte string) EntriesOption {
	return func(o *entriesOptions) {
		o.lte = lte
	}
}

type Entry struct {
	Key   string
	Value ipld.Link
}

func Entries(ctx context.Context, blocks block.Fetcher, root ipld.Link, opts ...EntriesOption) iter.Seq2[Entry, error] {
	o := &entriesOptions{}
	for _, opt := range opts {
		opt(o)
	}

	hasKeyPrefix := isKeyPrefix(o)
	hasKeyRange := isKeyRange(o)
	hasKeyLowerBoundRange := hasKeyRange && isKeyLowerBoundRange(o)
	hasKeyLowerBoundRangeInclusive := hasKeyLowerBoundRange && isKeyLowerBoundRangeInclusive(o)
	hasKeyLowerBoundRangeExclusive := hasKeyLowerBoundRange && isKeyLowerBoundRangeExclusive(o)
	hasKeyUpperBoundRange := hasKeyRange && isKeyUpperBoundRange(o)
	hasKeyUpperBoundRangeInclusive := hasKeyUpperBoundRange && isKeyUpperBoundRangeInclusive(o)
	hasKeyUpperBoundRangeExclusive := hasKeyUpperBoundRange && isKeyUpperBoundRangeExclusive(o)
	hasKeyUpperAndLowerBoundRange := hasKeyLowerBoundRange && hasKeyUpperBoundRange

	shards := shard.NewFetcher(blocks)
	rshard, err := shards.GetRoot(ctx, root)
	if err != nil {
		return func(yield func(Entry, error) bool) {
			yield(Entry{}, fmt.Errorf("getting root: %w", err))
		}
	}

	var ents func(s block.BlockView[shard.Shard]) iter.Seq2[Entry, error]
	ents = func(s block.BlockView[shard.Shard]) iter.Seq2[Entry, error] {
		return func(yield func(Entry, error) bool) {
			for _, entry := range s.Value().Entries() {
				key := s.Value().Prefix() + entry.Key()

				if entry.Value().Shard() != nil {
					if entry.Value().Value() != nil {
						if (hasKeyPrefix && strings.HasPrefix(key, o.prefix)) ||
							(hasKeyUpperAndLowerBoundRange && (((hasKeyLowerBoundRangeExclusive && key > o.gt) || (hasKeyLowerBoundRangeInclusive && key >= o.gte)) && ((hasKeyUpperBoundRangeExclusive && key < o.lt) || (hasKeyUpperBoundRangeInclusive && key <= o.lte)))) ||
							(hasKeyLowerBoundRangeExclusive && key > o.gt) ||
							(hasKeyLowerBoundRangeInclusive && key >= o.gte) ||
							(hasKeyUpperBoundRangeExclusive && key < o.lt) ||
							(hasKeyUpperBoundRangeInclusive && key <= o.lte) ||
							(!hasKeyPrefix && !hasKeyRange) {
							yield(Entry{key, entry.Value().Value()}, nil)
						}
					}

					if hasKeyPrefix {
						if len(o.prefix) <= len(key) && !strings.HasPrefix(key, o.prefix) {
							continue
						}
						if len(o.prefix) > len(key) && !strings.HasPrefix(o.prefix, key) {
							continue
						}
					} else if (hasKeyLowerBoundRangeExclusive && (trunc(key, min(len(key), len(o.gt))) < trunc(o.gt, min(len(key), len(o.gt))))) ||
						(hasKeyLowerBoundRangeInclusive && (trunc(key, min(len(key), len(o.gte))) < trunc(o.gte, min(len(key), len(o.gte))))) ||
						(hasKeyUpperBoundRangeExclusive && (trunc(key, min(len(key), len(o.lt))) > trunc(o.lt, min(len(key), len(o.lt))))) ||
						(hasKeyUpperBoundRangeInclusive && (trunc(key, min(len(key), len(o.lte))) > trunc(o.lte, min(len(key), len(o.lte))))) {
						continue
					}

					c, err := shards.Get(ctx, entry.Value().Shard())
					if err != nil {
						yield(Entry{}, fmt.Errorf("getting shard: %w", err))
						return
					}

					for entry, err := range ents(c) {
						if !yield(entry, err) || err != nil {
							return
						}
					}
				} else {
					if (hasKeyPrefix && strings.HasPrefix(key, o.prefix)) ||
						(hasKeyRange && hasKeyUpperAndLowerBoundRange && (((hasKeyLowerBoundRangeExclusive && key > o.gt) || (hasKeyLowerBoundRangeInclusive && key >= o.gte)) &&
							((hasKeyUpperBoundRangeExclusive && key < o.lt) || (hasKeyUpperBoundRangeInclusive && key <= o.lte)))) ||
						(hasKeyRange && !hasKeyUpperAndLowerBoundRange && ((hasKeyLowerBoundRangeExclusive && key > o.gt) || (hasKeyLowerBoundRangeInclusive && key >= o.gte) ||
							(hasKeyUpperBoundRangeExclusive && key < o.lt) || (hasKeyUpperBoundRangeInclusive && key <= o.lte))) ||
						(!hasKeyPrefix && !hasKeyRange) {
						if !yield(Entry{key, entry.Value().Value()}, nil) {
							return
						}
					}
				}
			}
		}
	}
	return ents(shard.AsBlock(rshard))
}

func isKeyPrefix(o *entriesOptions) bool {
	return o.prefix != ""
}

func isKeyRange(o *entriesOptions) bool {
	return isKeyUpperBoundRange(o) || isKeyLowerBoundRange(o)
}

func isKeyLowerBoundRange(o *entriesOptions) bool {
	return isKeyLowerBoundRangeInclusive(o) || isKeyLowerBoundRangeExclusive(o)
}

func isKeyLowerBoundRangeInclusive(o *entriesOptions) bool {
	return o.gte != ""
}

func isKeyLowerBoundRangeExclusive(o *entriesOptions) bool {
	return o.gt != ""
}

func isKeyUpperBoundRange(o *entriesOptions) bool {
	return isKeyUpperBoundRangeInclusive(o) || isKeyUpperBoundRangeExclusive(o)
}

func isKeyUpperBoundRangeInclusive(o *entriesOptions) bool {
	return o.lte != ""
}

func isKeyUpperBoundRangeExclusive(o *entriesOptions) bool {
	return o.lt != ""
}

func trunc(str string, l int) string {
	if len(str) <= l {
		return str
	}
	return str[:l]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
