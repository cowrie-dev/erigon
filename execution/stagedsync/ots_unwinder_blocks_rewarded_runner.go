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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
)

func RunBlocksRewardedBlockUnwind(ctx context.Context, tx kv.RwTx, blockReader services.FullBlockReader, isShortInterval bool, logEvery *time.Ticker, u *UnwindState, unwinder IndexUnwinder) error {
	// The unwind interval is ]u.UnwindPoint, EOF]
	startBlock := u.UnwindPoint + 1

	for blockNum := startBlock; blockNum <= u.CurrentBlockNumber; blockNum++ {
		hash, _, err := blockReader.CanonicalHash(ctx, tx, blockNum)
		if err != nil {
			return err
		}
		header, err := blockReader.HeaderByHash(ctx, tx, hash)
		if err != nil {
			return err
		}

		if err := unwinder.UnwindAddress(tx, header.Coinbase, header.Number.Uint64()); err != nil {
			return err
		}

		select {
		default:
		case <-ctx.Done():
			return common.ErrStopped
		case <-logEvery.C:
			log.Info("Unwinding blocks rewarded indexer", "blockNum", blockNum)
		}
	}

	return nil
}
