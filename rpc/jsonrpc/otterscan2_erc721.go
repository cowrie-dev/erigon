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
	"bytes"
	"context"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/abi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/stagedsync/otscontracts"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
)

type ERC721Match struct {
	Block   *hexutil.Uint64 `json:"blockNumber"`
	Address *common.Address `json:"address"`
	Name    string          `json:"name"`
	Symbol  string          `json:"symbol"`
}

func (api *Otterscan2APIImpl) GetERC721List(ctx context.Context, idx, count uint64) (*ContractListResult, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	res, err := api.genericMatchingList(ctx, tx, kv.OtsERC721, kv.OtsERC721Counter, idx, count)
	if err != nil {
		return nil, err
	}

	extraData, err := api.newERC721ExtraData(ctx)
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

func (api *Otterscan2APIImpl) newERC721ExtraData(ctx context.Context) (ExtraDataExtractor, error) {
	erc721ABI, err := abi.JSON(bytes.NewReader(otscontracts.ERC20))
	if err != nil {
		return nil, err
	}

	name, err := erc721ABI.Pack("name")
	if err != nil {
		return nil, err
	}
	symbol, err := erc721ABI.Pack("symbol")
	if err != nil {
		return nil, err
	}

	return func(tx kv.Tx, res *AddrMatch, addr common.Address, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, stateReader state.StateReader) (interface{}, error) {
		// name()
		retName, err := decodeReturnData(ctx, &addr, name, "name", header, evm, chainConfig, ibs, &erc721ABI)
		if err != nil {
			return nil, err
		}
		strName := "<ERROR>"
		if retName != nil {
			strName = retName.(string)
		}

		// symbol()
		retSymbol, err := decodeReturnData(ctx, &addr, symbol, "symbol", header, evm, chainConfig, ibs, &erc721ABI)
		if err != nil {
			return nil, err
		}
		strSymbol := "<ERROR>"
		if retSymbol != nil {
			strSymbol = retSymbol.(string)
		}

		return &ERC721Match{
			res.Block,
			res.Address,
			strName,
			strSymbol,
		}, nil
	}, nil
}

func (api *Otterscan2APIImpl) GetERC721Count(ctx context.Context) (uint64, error) {
	return api.genericMatchingCounter(ctx, kv.OtsERC721Counter)
}
