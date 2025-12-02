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

package jsonrpc

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
)

type ContractListResult struct {
	BlocksSummary map[hexutil.Uint64]*BlockSummary `json:"blocksSummary"`
	Results       []interface{}                    `json:"results"`
}

func (api *Otterscan2APIImpl) genericMatchingList(ctx context.Context, tx kv.Tx, matchTable, counterTable string, idx, count uint64) ([]AddrMatch, error) {
	if count > MAX_MATCH_COUNT {
		return nil, fmt.Errorf("maximum allowed results: %v", MAX_MATCH_COUNT)
	}

	if tx == nil {
		var err error
		tx, err = api.db.BeginRo(ctx)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	c, err := tx.Cursor(counterTable)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	startIdx := idx + 1
	counterK, blockNumV, err := c.Seek(hexutil.EncodeTs(startIdx))
	if err != nil {
		return nil, err
	}
	if counterK == nil {
		return nil, nil
	}

	prevCounterK, _, err := c.Prev()
	if err != nil {
		return nil, err
	}
	prevTotal := uint64(0)
	if prevCounterK != nil {
		prevTotal = binary.BigEndian.Uint64(prevCounterK)
	}

	contracts, err := tx.CursorDupSort(matchTable)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	kk, vv, err := contracts.SeekExact(blockNumV)
	if err != nil {
		return nil, err
	}
	if kk == nil {
		// DB corrupted
		return nil, fmt.Errorf("couldn't find exact block %v for counter index: %v, counter key: %v", binary.BigEndian.Uint64(blockNumV), startIdx, binary.BigEndian.Uint64(counterK))
	}

	// Position cursor at the first match
	for i := uint64(0); i < startIdx-prevTotal-1; i++ {
		kk, vv, err = contracts.NextDup()
		if err != nil {
			return nil, err
		}
	}

	matches := make([]AddrMatch, 0, count)
	for i := uint64(0); i < count && kk != nil; i++ {
		blockNum := hexutil.Uint64(binary.BigEndian.Uint64(kk))
		addr := common.BytesToAddress(vv)
		matches = append(matches, AddrMatch{Block: &blockNum, Address: &addr})

		kk, vv, err = contracts.NextDup()
		if err != nil {
			return nil, err
		}
		if kk == nil {
			kk, vv, err = contracts.NextNoDup()
			if err != nil {
				return nil, err
			}
			if kk == nil {
				break
			}
		}
	}

	return matches, nil
}

func (api *Otterscan2APIImpl) genericMatchingCounter(ctx context.Context, counterTable string) (uint64, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	ct, err := tx.Cursor(counterTable)
	if err != nil {
		return 0, err
	}
	defer ct.Close()

	k, _, err := ct.Last()
	if err != nil {
		return 0, err
	}
	if k == nil {
		return 0, nil
	}

	counter := binary.BigEndian.Uint64(k)
	return counter, nil
}
