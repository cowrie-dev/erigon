// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package stagedsync

import (
	"context"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
)

func BlocksRewardedExecutor(ctx context.Context, db kv.RoDB, tx kv.RwTx, isInternalTx bool, tmpDir string, chainConfig *chain.Config, blockReader services.FullBlockReader, engine rules.Engine, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, s *StageState, logger log.Logger) (uint64, error) {
	blocksRewardedHandler := NewBlocksRewardedIndexerHandler(tmpDir, s, logger)
	defer blocksRewardedHandler.Close()

	return runIncrementalHeaderIndexerExecutor(db, tx, blockReader, startBlock, endBlock, isShortInterval, logEvery, ctx, s, blocksRewardedHandler)
}

// Implements HeaderIndexerHandler interface in order to index blocks rewarded
type BlocksRewardedIndexerHandler struct {
	IndexHandler
}

func NewBlocksRewardedIndexerHandler(tmpDir string, s *StageState, logger log.Logger) HeaderIndexerHandler {
	collector := etl.NewCollector(s.LogPrefix(), tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	bitmaps := map[string]*roaring64.Bitmap{}

	return &BlocksRewardedIndexerHandler{
		&StandardIndexHandler{kv.OtsBlocksRewardedIndex, kv.OtsBlocksRewardedCounter, collector, bitmaps},
	}
}

// Index fee recipient address -> blockNum
func (h *BlocksRewardedIndexerHandler) HandleMatch(header *types.Header) {
	h.TouchIndex(header.Coinbase, header.Number.Uint64())
}
