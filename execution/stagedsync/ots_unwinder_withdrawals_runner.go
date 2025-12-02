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
	"fmt"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
)

func RunWithdrawalsBlockUnwind(ctx context.Context, tx kv.RwTx, blockReader services.FullBlockReader, isShortInterval bool, logEvery *time.Ticker, u *UnwindState, unwinder IndexUnwinder) error {
	// The unwind interval is ]u.UnwindPoint, EOF]
	startBlock := u.UnwindPoint + 1

	idx2Block, err := tx.RwCursor(kv.OtsWithdrawalIdx2Block)
	if err != nil {
		return err
	}
	defer idx2Block.Close()

	// In order to unwind idx2Block, we need to find the max withdrawal ID from the unwind point
	// block or less
	blockNum := u.UnwindPoint
	found := false
	withdrawalId := uint64(0)
	for blockNum > 0 {
		hash, _, err := blockReader.CanonicalHash(ctx, tx, blockNum)
		if err != nil {
			return err
		}
		body, _, err := blockReader.Body(ctx, tx, hash, blockNum)
		if err != nil {
			return err
		}

		withdrawalsAmount := len(body.Withdrawals)
		if withdrawalsAmount > 0 {
			found = true
			lastWithdrawal := body.Withdrawals[withdrawalsAmount-1]
			withdrawalId = lastWithdrawal.Index
			break
		}

		blockNum--
	}

	// Unwind idx2Block
	if found {
		unwoundToIndex, err := unwindUint64KeyBasedTable(idx2Block, withdrawalId)
		if err != nil {
			return err
		}

		// withdrawal ID MUST exist in idx2Block, otherwise it is a DB inconsistency
		if unwoundToIndex != withdrawalId {
			return fmt.Errorf("couldn't find bucket=%s k=%v to unwind; probably DB corruption", kv.OtsWithdrawalIdx2Block, withdrawalId)
		}
	}

	for blockNum := startBlock; blockNum <= u.CurrentBlockNumber; blockNum++ {
		hash, _, err := blockReader.CanonicalHash(ctx, tx, blockNum)
		if err != nil {
			return err
		}
		body, _, err := blockReader.Body(ctx, tx, hash, blockNum)
		if err != nil {
			return err
		}

		if len(body.Withdrawals) == 0 {
			continue
		}
		for _, w := range body.Withdrawals {
			if err := unwinder.UnwindAddress(tx, w.Address, w.Index); err != nil {
				return err
			}
		}

		select {
		default:
		case <-ctx.Done():
			return common.ErrStopped
		case <-logEvery.C:
			log.Info("Unwinding withdrawals indexer", "blockNum", blockNum)
		}
	}

	return nil
}
