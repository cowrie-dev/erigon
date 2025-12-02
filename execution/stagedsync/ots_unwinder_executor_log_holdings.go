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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
)

func NewGenericLogHoldingsUnwinder() UnwindExecutor {
	return func(ctx context.Context, tx kv.RwTx, u *UnwindState, blockReader services.FullBlockReader, isShortInterval bool, logEvery *time.Ticker) error {
		erc20Unwinder, err := NewTransferLogHoldingsUnwinder(tx, kv.OtsERC20Holdings, false)
		if err != nil {
			return err
		}
		defer erc20Unwinder.Dispose()

		erc721Unwinder, err := NewTransferLogHoldingsUnwinder(tx, kv.OtsERC721Holdings, true)
		if err != nil {
			return err
		}
		defer erc721Unwinder.Dispose()

		return runLogUnwind(ctx, tx, blockReader, isShortInterval, logEvery, u, TRANSFER_TOPIC, []UnwindHandler{erc20Unwinder, erc721Unwinder})
	}
}

type TransferLogHoldingsUnwinder struct {
	indexBucket string
	isNFT       bool
	target      kv.RwCursorDupSort
	targetDel   kv.RwCursorDupSort
}

func NewTransferLogHoldingsUnwinder(tx kv.RwTx, indexBucket string, isNFT bool) (*TransferLogHoldingsUnwinder, error) {
	target, err := tx.RwCursorDupSort(indexBucket)
	if err != nil {
		return nil, err
	}

	targetDel, err := tx.RwCursorDupSort(indexBucket)
	if err != nil {
		return nil, err
	}

	return &TransferLogHoldingsUnwinder{
		indexBucket,
		isNFT,
		target,
		targetDel,
	}, nil
}

func (u *TransferLogHoldingsUnwinder) Dispose() error {
	u.target.Close()
	u.targetDel.Close()

	return nil
}

func (u *TransferLogHoldingsUnwinder) Unwind(tx kv.RwTx, results []*TransferAnalysisResult, ethTx uint64) error {
	for _, r := range results {
		if err := r.UnwindHolding(tx, u.isNFT, u, ethTx); err != nil {
			return err
		}
	}

	return nil
}

func (u *TransferLogHoldingsUnwinder) UnwindAddress(tx kv.RwTx, addr common.Address, ethTx uint64) error {
	return fmt.Errorf("NOT IMPLEMENTED")
}

func (u *TransferLogHoldingsUnwinder) UnwindAddressHolding(tx kv.RwTx, addr, token common.Address, ethTx uint64) error {
	k := addr.Bytes()
	v, err := u.target.SeekBothRange(k, token.Bytes())
	if err != nil {
		return err
	}
	if k == nil {
		return nil
	}
	if !bytes.HasPrefix(v, token.Bytes()) {
		return nil
	}
	existingEthTx := binary.BigEndian.Uint64(v[length.Addr:])

	// ignore touches after the first recognized holding occurrence
	if ethTx > existingEthTx {
		return nil
	}

	// touches before the first recognized holding occurrence means DB corruption
	if ethTx < existingEthTx {
		return fmt.Errorf("db possibly corrupted: trying to unwind bucket=%s holder=%s token=%s ethTx=%d existingEthTx=%d", u.indexBucket, addr, token, ethTx, existingEthTx)
	}

	if err := u.targetDel.DeleteExact(k, v); err != nil {
		return err
	}

	return nil
}
