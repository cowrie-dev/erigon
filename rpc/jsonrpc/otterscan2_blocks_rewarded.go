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
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

type BlocksRewardedListResult struct {
	BlocksSummary map[hexutil.Uint64]*BlockSummary2 `json:"blocksSummary"`
	Results       []*BlocksRewardedMatch            `json:"results"`
}

type BlocksRewardedMatch struct {
	BlockNum hexutil.Uint64 `json:"blockNumber"`
	// Amount    hexutil.Uint64 `json:"amount"`
}

type blocksRewardedSearchResultMaterializer struct {
	blockReader services.FullBlockReader
}

func NewBlocksRewardedSearchResultMaterializer(tx kv.Tx, blockReader services.FullBlockReader) (*blocksRewardedSearchResultMaterializer, error) {
	return &blocksRewardedSearchResultMaterializer{blockReader}, nil
}

func (w *blocksRewardedSearchResultMaterializer) Convert(ctx context.Context, tx kv.Tx, idx uint64) (*BlocksRewardedMatch, error) {
	// hash, err := w.blockReader.CanonicalHash(ctx, tx, blockNum)
	// if err != nil {
	// 	return nil, err
	// }
	// TODO: replace by header
	// body, _, err := w.blockReader.Body(ctx, tx, hash, blockNum)
	// if err != nil {
	// 	return nil, err
	// }

	result := &BlocksRewardedMatch{
		BlockNum: hexutil.Uint64(idx),
	}
	return result, nil
}

func (api *Otterscan2APIImpl) GetBlocksRewardedList(ctx context.Context, addr common.Address, idx, count uint64) (*BlocksRewardedListResult, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	srm, err := NewBlocksRewardedSearchResultMaterializer(tx, api._blockReader)
	if err != nil {
		return nil, err
	}

	ret, err := genericResultList(ctx, tx, addr, idx, count, kv.OtsBlocksRewardedIndex, kv.OtsBlocksRewardedCounter, (SearchResultMaterializer[BlocksRewardedMatch])(srm))
	if err != nil {
		return nil, err
	}

	blocks := make([]hexutil.Uint64, 0, len(ret))
	for _, r := range ret {
		blocks = append(blocks, hexutil.Uint64(r.BlockNum))
	}

	blocksSummary, err := api.newBlocksSummary2FromResults(ctx, tx, blocks)
	if err != nil {
		return nil, err
	}
	return &BlocksRewardedListResult{
		BlocksSummary: blocksSummary,
		Results:       ret,
	}, nil
}

func (api *Otterscan2APIImpl) GetBlocksRewardedCount(ctx context.Context, addr common.Address) (uint64, error) {
	return api.genericGetCount(ctx, addr, kv.OtsBlocksRewardedCounter)
}

func (api *Otterscan2APIImpl) getBlockWithSenders(ctx context.Context, number rpc.BlockNumber, tx kv.Tx) (*types.Block, []common.Address, error) {
	if number == rpc.PendingBlockNumber {
		return api.pendingBlock(), nil, nil
	}

	n, hash, _, err := rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(number), tx, api._blockReader, api.filters)
	if err != nil {
		return nil, nil, err
	}

	block, senders, err := api._blockReader.BlockWithSenders(ctx, tx, hash, n)
	return block, senders, err
}

func (api *Otterscan2APIImpl) getBlockDetailsImpl(ctx context.Context, tx kv.Tx, b *types.Block, number rpc.BlockNumber, senders []common.Address) (*BlockSummary2, error) {
	var response BlockSummary2
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	getIssuanceRes, err := delegateIssuance(tx, b, chainConfig, api.engine())
	if err != nil {
		return nil, err
	}
	receipts, err := api.getReceipts(ctx, tx.(kv.TemporalTx), b)
	if err != nil {
		return nil, fmt.Errorf("getReceipts error: %v", err)
	}
	feesRes, err := delegateBlockFees(ctx, tx, b, senders, chainConfig, receipts)
	if err != nil {
		return nil, err
	}

	response.Block = hexutil.Uint64(b.Number().Uint64())
	response.Time = b.Time()
	response.internalIssuance = getIssuanceRes
	response.TotalFees = (*hexutil.Big)(feesRes)
	return &response, nil
}
