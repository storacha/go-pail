package block

import (
	"context"

	"github.com/ipld/go-ipld-prime"
)

// TieredBlockFetcher is a [Fetcher] that attempts to retrieve a block serially
// from multiple configured fetchers in order, returning the first [Block] that
// is returned by a fetcher.
type TieredBlockFetcher struct {
	fetchers []Fetcher
}

func (mbf *TieredBlockFetcher) Get(ctx context.Context, link ipld.Link) (Block, error) {
	var ferr error
	for _, f := range mbf.fetchers {
		v, err := f.Get(ctx, link)
		if err != nil {
			ferr = err
			continue
		}
		return v, nil
	}
	return nil, ferr
}

// NewTieredBlockFetcher cretaes a new [TieredBlockFetcher] - a [Fetcher] that
// attempts to retrieve a block serially from multiple configured fetchers in
// order, returning the first [Block] that is returned by a fetcher.
func NewTieredBlockFetcher(fetchers ...Fetcher) *TieredBlockFetcher {
	return &TieredBlockFetcher{fetchers}
}
