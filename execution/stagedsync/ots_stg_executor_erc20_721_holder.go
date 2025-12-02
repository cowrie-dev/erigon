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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
)

func ERC20And721HolderIndexerExecutor(ctx context.Context, db kv.RoDB, tx kv.RwTx, isInternalTx bool, tmpDir string, chainConfig *chain.Config, blockReader services.FullBlockReader, engine rules.Engine, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, s *StageState, logger log.Logger) (uint64, error) {
	analyzer, err := NewTransferLogAnalyzer()
	if err != nil {
		return startBlock, err
	}

	aggrHandler := NewMultiIndexerHandler[TransferAnalysisResult](
		NewTransferLogHolderHandler(tmpDir, s, false, kv.OtsERC20Holdings, logger),
		NewTransferLogHolderHandler(tmpDir, s, true, kv.OtsERC721Holdings, logger),
	)
	defer aggrHandler.Close()

	if startBlock == 0 && isInternalTx {
		return runConcurrentLogIndexerExecutor[TransferAnalysisResult](db, tx, blockReader, startBlock, endBlock, isShortInterval, logEvery, ctx, s, analyzer, aggrHandler)
	}
	return runIncrementalLogIndexerExecutor[TransferAnalysisResult](db, tx, blockReader, startBlock, endBlock, isShortInterval, logEvery, ctx, s, analyzer, aggrHandler)
}
