package pail

import (
	"context"
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/storacha/go-pail/block"
	"github.com/storacha/go-pail/shard"
)

func TestBasicExample(t *testing.T) {
	ctx := context.Background()

	rootBlock, _ := shard.MarshalBlock(shard.NewRoot(nil))

	blocks := block.NewMapBlockstore()
	_ = blocks.Put(ctx, rootBlock)

	fmt.Printf("Root: %s\n", rootBlock.Link())

	key := "room-guardian.jpg"
	value := cidlink.Link{Cid: cid.MustParse("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")}

	fmt.Printf("Putting %s: %s\n", key, value)

	root, diff, _ := Put(ctx, blocks, rootBlock.Link(), key, value)

	fmt.Printf("Root: %s\n", root)
	fmt.Println("Added blocks:")
	for _, b := range diff.Additions {
		fmt.Printf("+ %s\n", b.Link())
		_ = blocks.Put(ctx, b)
	}
	fmt.Println("Removed blocks:")
	for _, b := range diff.Removals {
		fmt.Printf("- %s\n", b.Link())
		_ = blocks.Del(ctx, b.Link())
	}

	fmt.Println("Entries:")
	for entry := range Entries(ctx, blocks, root) {
		fmt.Printf("%s: %s\n", entry.Key, entry.Value)
	}
}
