package block

import (
	"context"
	"iter"
	"sync"

	"github.com/ipld/go-ipld-prime"
)

// MapBlockstore is a blockstore that is backed by an in memory map.
type MapBlockstore struct {
	data  map[ipld.Link]Block
	mutex sync.RWMutex
}

func (bs *MapBlockstore) Get(ctx context.Context, link ipld.Link) (Block, error) {
	bs.mutex.RLock()
	defer bs.mutex.RUnlock()

	b, ok := bs.data[link]
	if !ok {
		return nil, ErrNotFound
	}
	return b, nil
}

func (bs *MapBlockstore) Put(ctx context.Context, b Block) error {
	bs.mutex.Lock()
	bs.data[b.Link()] = b
	bs.mutex.Unlock()
	return nil
}

func (bs *MapBlockstore) Del(ctx context.Context, link ipld.Link) error {
	bs.mutex.Lock()
	delete(bs.data, link)
	bs.mutex.Unlock()
	return nil
}

func (bs *MapBlockstore) Entries(ctx context.Context) iter.Seq2[Block, error] {
	return func(yield func(Block, error) bool) {
		bs.mutex.RLock()
		defer bs.mutex.RUnlock()
		for _, b := range bs.data {
			if !yield(b, nil) {
				return
			}
		}
	}
}

// NewMapBlockstore creates a new blockstore that is backed by an in memory map.
func NewMapBlockstore() *MapBlockstore {
	return &MapBlockstore{map[ipld.Link]Block{}, sync.RWMutex{}}
}
