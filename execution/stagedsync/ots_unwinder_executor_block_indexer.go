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
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
)

type BlockUnwinderRunner func(ctx context.Context, tx kv.RwTx, blockReader services.FullBlockReader, isShortInterval bool, logEvery *time.Ticker, u *UnwindState, unwinder IndexUnwinder) error

func NewGenericBlockIndexerUnwinder(bucket, counterBucket string, unwinderRunner BlockUnwinderRunner) UnwindExecutor {
	return func(ctx context.Context, tx kv.RwTx, u *UnwindState, blockReader services.FullBlockReader, isShortInterval bool, logEvery *time.Ticker) error {
		unwinder, err := newBlockIndexerUnwinder(tx, bucket, counterBucket)
		if err != nil {
			return err
		}
		defer unwinder.Dispose()

		return unwinderRunner(ctx, tx, blockReader, isShortInterval, logEvery, u, unwinder)
	}
}

type BlockIndexerIndexerUnwinder struct {
	indexBucket   string
	counterBucket string
	target        kv.RwCursor
	targetDel     kv.RwCursor
	counter       kv.RwCursorDupSort
}

func newBlockIndexerUnwinder(tx kv.RwTx, indexBucket, counterBucket string) (*BlockIndexerIndexerUnwinder, error) {
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

	return &BlockIndexerIndexerUnwinder{
		indexBucket,
		counterBucket,
		target,
		targetDel,
		counter,
	}, nil
}

func (u *BlockIndexerIndexerUnwinder) UnwindAddress(tx kv.RwTx, addr common.Address, ethTx uint64) error {
	return unwindAddress(tx, u.target, u.targetDel, u.counter, u.indexBucket, u.counterBucket, addr, ethTx)
}

func (u *BlockIndexerIndexerUnwinder) Dispose() error {
	u.target.Close()
	u.targetDel.Close()
	u.counter.Close()

	return nil
}
