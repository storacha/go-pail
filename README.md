# go-pail

DAG based key value store. Sharded DAG that minimises traversals and work to build shards.

* 📖 [Read the SPEC](https://github.com/web3-storage/specs/blob/4163e28d7e6a7c44cff68db9d9bffb9b37707dc6/pail.md).
* 🎬 [Watch the Presentation](https://youtu.be/f-BrtpYKZfg).

## Install

```sh
go get github.com/storacha/go-pail
```

## Usage

```go
package main

import (
  "context"
  "fmt"

  "github.com/ipfs/go-cid"
  cidlink "github.com/ipld/go-ipld-prime/linking/cid"
  "github.com/storacha/go-pail"
  "github.com/storacha/go-pail/block"
  "github.com/storacha/go-pail/shard"
)

func main() {
  ctx := context.Background()

  rootBlock, _ := shard.MarshalBlock(shard.NewRoot(nil))

  blocks := block.NewMapBlockstore()
  _ = blocks.Put(ctx, rootBlock)

  fmt.Printf("Root: %s\n",  rootBlock.Link())

  key := "room-guardian.jpg"
  value := cidlink.Link{Cid: cid.MustParse("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")}

  fmt.Printf("Putting %s: %s\n", key, value)

  root, diff, _ := pail.Put(ctx, blocks, rootBlock.Link(), key, value)

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
  for entry := range pail.Entries(ctx, blocks, root) {
    fmt.Printf("%s: %s\n", entry.Key, entry.Value)
  }
}

// Output:
//
// Root: bafyreiesj77bspnvajezltkvavgngyve7pucqcx527s42jzmo66tdewg44
// Putting room-guardian.jpg: bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy
// Root: bafyreidijignftc2p2dgt32eu2d5ge24jo74xxbmcn4clxuvfd52wo6sve
// Added blocks:
// + bafyreidijignftc2p2dgt32eu2d5ge24jo74xxbmcn4clxuvfd52wo6sve
// Removed blocks:
// - bafyreiesj77bspnvajezltkvavgngyve7pucqcx527s42jzmo66tdewg44
// Entries:
// room-guardian.jpg: bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy
```

## Contributing

Feel free to join in. All welcome. [Open an issue](https://github.com/storacha/go-pail/issues)!

## License

Dual-licensed under [MIT or Apache 2.0](https://github.com/storacha/go-pail/blob/main/LICENSE.md)
