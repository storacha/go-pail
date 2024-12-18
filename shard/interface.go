package shard

import (
	"github.com/storacha/go-pail/block"
)

type BlockView interface {
	block.BlockView[Shard]
}

type RootBlockView interface {
	block.BlockView[RootShard]
}
