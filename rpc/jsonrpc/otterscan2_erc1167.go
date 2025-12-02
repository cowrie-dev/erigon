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
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
)

type ERC1167Match struct {
	Block          *hexutil.Uint64 `json:"blockNumber"`
	Address        *common.Address `json:"address"`
	Implementation *common.Address `json:"implementation"`
}

func (api *Otterscan2APIImpl) GetERC1167List(ctx context.Context, idx, count uint64) (*ContractListResult, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	res, err := api.genericMatchingList(ctx, tx, kv.OtsERC1167, kv.OtsERC1167Counter, idx, count)
	if err != nil {
		return nil, err
	}

	extraData, err := api.newERC1167ExtraData(ctx)
	if err != nil {
		return nil, err
	}

	results, err := api.genericExtraData(ctx, tx, res, extraData)
	if err != nil {
		return nil, err
	}
	blocksSummary, err := api.newBlocksSummaryFromResults(ctx, tx, ToBlockSlice(res))
	if err != nil {
		return nil, err
	}
	return &ContractListResult{
		BlocksSummary: blocksSummary,
		Results:       results,
	}, nil
}

func (api *Otterscan2APIImpl) newERC1167ExtraData(ctx context.Context) (ExtraDataExtractor, error) {
	return func(tx kv.Tx, res *AddrMatch, addr common.Address, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, stateReader state.StateReader) (interface{}, error) {
		code, err := stateReader.ReadAccountCode(addr)
		if err != nil {
			return nil, err
		}

		impl := common.BytesToAddress(code[10:30])

		return &ERC1167Match{
			res.Block,
			res.Address,
			&impl,
		}, nil
	}, nil
}

func (api *Otterscan2APIImpl) GetERC1167Count(ctx context.Context) (uint64, error) {
	return api.genericMatchingCounter(ctx, kv.OtsERC1167Counter)
}

func (api *Otterscan2APIImpl) GetERC1167Impl(ctx context.Context, addr common.Address) (common.Address, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return common.Address{}, err
	}
	defer tx.Rollback()

	// TODO(ots2-rebase): CreateStateReader signature changed completely, needs TemporalTx
	// For now, stub out to get compilation working
	return common.Address{}, fmt.Errorf("GetERC1167Impl not yet ported - CreateStateReader API changed")
}
