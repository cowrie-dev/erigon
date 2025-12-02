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
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
)

func NewGenericLogIndexerUnwinder() UnwindExecutor {
	return func(ctx context.Context, tx kv.RwTx, u *UnwindState, blockReader services.FullBlockReader, isShortInterval bool, logEvery *time.Ticker) error {
		erc20Unwinder, err := NewTransferLogIndexerUnwinder(tx, kv.OtsERC20TransferIndex, kv.OtsERC20TransferCounter, false)
		if err != nil {
			return err
		}
		defer erc20Unwinder.Dispose()

		erc721Unwinder, err := NewTransferLogIndexerUnwinder(tx, kv.OtsERC721TransferIndex, kv.OtsERC721TransferCounter, true)
		if err != nil {
			return err
		}
		defer erc721Unwinder.Dispose()

		return runLogUnwind(ctx, tx, blockReader, isShortInterval, logEvery, u, TRANSFER_TOPIC, []UnwindHandler{erc20Unwinder, erc721Unwinder})
	}
}

type LogIndexerUnwinder interface {
	UnwindAddress(tx kv.RwTx, addr common.Address, ethTx uint64) error
	UnwindAddressHolding(tx kv.RwTx, addr, token common.Address, ethTx uint64) error
	Dispose() error
}

type UnwindHandler interface {
	Unwind(tx kv.RwTx, results []*TransferAnalysisResult, ethTx uint64) error
}

type TransferLogIndexerUnwinder struct {
	indexBucket   string
	counterBucket string
	isNFT         bool
	target        kv.RwCursor
	targetDel     kv.RwCursor
	counter       kv.RwCursorDupSort
}

func NewTransferLogIndexerUnwinder(tx kv.RwTx, indexBucket, counterBucket string, isNFT bool) (*TransferLogIndexerUnwinder, error) {
	target, err := tx.RwCursor(indexBucket)
	if err != nil {
		return nil, err
	}

	targetDel, err := tx.RwCursor(indexBucket)
	if err != nil {
		return nil, err
	}

	counter, err := tx.RwCursorDupSort(counterBucket)
	if err != nil {
		return nil, err
	}

	return &TransferLogIndexerUnwinder{
		indexBucket,
		counterBucket,
		isNFT,
		target,
		targetDel,
		counter,
	}, nil
}

func (u *TransferLogIndexerUnwinder) Dispose() error {
	u.target.Close()
	u.targetDel.Close()
	u.counter.Close()

	return nil
}

func runLogUnwind(ctx context.Context, tx kv.RwTx, blockReader services.FullBlockReader, isShortInterval bool, logEvery *time.Ticker, u *UnwindState, topic []byte, unwinders []UnwindHandler) error {
	// TODO(ots2-rebase): kv.Log table removed
	return fmt.Errorf("log unwinder not yet ported - kv.Log removed")
}

// Unwind implements UnwindHandler interface
func (u *TransferLogIndexerUnwinder) Unwind(tx kv.RwTx, results []*TransferAnalysisResult, ethTx uint64) error {
	// TODO(ots2-rebase): This needs to be implemented for log-based unwinding
	return fmt.Errorf("TransferLogIndexerUnwinder.Unwind not yet ported")
}

func (u *TransferLogIndexerUnwinder) UnwindAddress(tx kv.RwTx, addr common.Address, ethTx uint64) error {
	return unwindAddress(tx, u.target, u.targetDel, u.counter, u.indexBucket, u.counterBucket, addr, ethTx)
}
