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
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
)

func WithdrawalsExecutor(ctx context.Context, db kv.RoDB, tx kv.RwTx, isInternalTx bool, tmpDir string, chainConfig *chain.Config, blockReader services.FullBlockReader, engine rules.Engine, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, s *StageState, logger log.Logger) (uint64, error) {
	withdrawalHandler, err := NewWithdrawalsIndexerHandler(tx, tmpDir, s, logger)
	if err != nil {
		return startBlock, err
	}
	defer withdrawalHandler.Close()

	return runIncrementalBodyIndexerExecutor(db, tx, blockReader, startBlock, endBlock, isShortInterval, logEvery, ctx, s, withdrawalHandler)
}

// Implements BodyIndexerHandler interface in order to index block withdrawals from CL
type WithdrawalsIndexerHandler struct {
	IndexHandler
	withdrawalIdx2Block kv.RwCursor
}

func NewWithdrawalsIndexerHandler(tx kv.RwTx, tmpDir string, s *StageState, logger log.Logger) (BodyIndexerHandler, error) {
	collector := etl.NewCollector(s.LogPrefix(), tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	bitmaps := map[string]*roaring64.Bitmap{}
	withdrawalIdx2Block, err := tx.RwCursor(kv.OtsWithdrawalIdx2Block)
	if err != nil {
		return nil, err
	}

	return &WithdrawalsIndexerHandler{
		&StandardIndexHandler{kv.OtsWithdrawalsIndex, kv.OtsWithdrawalsCounter, collector, bitmaps},
		withdrawalIdx2Block,
	}, nil
}

// Index all withdrawals from a block body;
// withdrawal address -> withdrawal index (NOT blockNum!!!)
func (h *WithdrawalsIndexerHandler) HandleMatch(blockNum uint64, body *types.Body) error {
	withdrawals := body.Withdrawals
	if len(withdrawals) == 0 {
		return nil
	}
	last := withdrawals[len(withdrawals)-1]

	k := hexutil.EncodeTs(last.Index)
	v := hexutil.EncodeTs(blockNum)
	if err := h.withdrawalIdx2Block.Put(k, v); err != nil {
		return err
	}

	for _, w := range withdrawals {
		h.TouchIndex(w.Address, w.Index)
	}

	return nil
}

func (h *WithdrawalsIndexerHandler) Close() {
	h.IndexHandler.Close()
	h.withdrawalIdx2Block.Close()
}
