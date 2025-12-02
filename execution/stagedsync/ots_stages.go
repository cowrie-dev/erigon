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

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

// Standard Otterscan V2 stages; if opted-in, they must be inserted before finish stage.
func OtsStages(ctx context.Context, caCfg ContractAnalyzerCfg) []*Stage {
	return []*Stage{
		{
			ID:          stages.OtsContractIndexer,
			Description: "Index contract creation",
			Forward:     GenericStageForwardFunc(ctx, caCfg, stages.Bodies, ContractIndexerExecutor),
			Unwind: GenericStageUnwindFunc(ctx, caCfg,
				NewGenericIndexerUnwinder(
					kv.OtsAllContracts,
					kv.OtsAllContractsCounter,
					nil,
				),
			),
			Prune: NoopStagePrune(ctx, caCfg),
		},
		{
			ID:          stages.OtsERC20Indexer,
			Description: "ERC20 token indexer",
			Forward: GenericStageForwardFunc(ctx, caCfg, stages.OtsContractIndexer,
				NewConcurrentIndexerExecutor(
					NewERC20Prober,
					kv.OtsAllContracts,
					kv.OtsERC20,
					kv.OtsERC20Counter,
				)),
			Unwind: GenericStageUnwindFunc(ctx, caCfg,
				NewGenericIndexerUnwinder(
					kv.OtsERC20,
					kv.OtsERC20Counter,
					roaring64.BitmapOf(kv.ADDR_ATTR_ERC20),
				)),
			Prune: NoopStagePrune(ctx, caCfg),
		},
		{
			ID:          stages.OtsERC165Indexer,
			Description: "ERC165 indexer",
			Forward: GenericStageForwardFunc(ctx, caCfg, stages.OtsContractIndexer,
				NewConcurrentIndexerExecutor(
					NewERC165Prober,
					kv.OtsAllContracts,
					kv.OtsERC165,
					kv.OtsERC165Counter,
				)),
			Unwind: GenericStageUnwindFunc(ctx, caCfg,
				NewGenericIndexerUnwinder(
					kv.OtsERC165,
					kv.OtsERC165Counter,
					roaring64.BitmapOf(kv.ADDR_ATTR_ERC165),
				)),
			Prune: NoopStagePrune(ctx, caCfg),
		},
		{
			ID:          stages.OtsERC721Indexer,
			Description: "ERC721 token indexer",
			Forward: GenericStageForwardFunc(ctx, caCfg, stages.OtsERC165Indexer,
				NewConcurrentIndexerExecutor(
					NewERC721Prober,
					kv.OtsERC165,
					kv.OtsERC721,
					kv.OtsERC721Counter,
				)),
			Unwind: GenericStageUnwindFunc(ctx, caCfg,
				NewGenericIndexerUnwinder(
					kv.OtsERC721,
					kv.OtsERC721Counter,
					roaring64.BitmapOf(kv.ADDR_ATTR_ERC721),
				)),
			Prune: NoopStagePrune(ctx, caCfg),
		},
		{
			ID:          stages.OtsERC1155Indexer,
			Description: "ERC1155 token indexer",
			Forward: GenericStageForwardFunc(ctx, caCfg, stages.OtsERC165Indexer,
				NewConcurrentIndexerExecutor(
					NewERC1155Prober,
					kv.OtsERC165,
					kv.OtsERC1155,
					kv.OtsERC1155Counter,
				)),
			Unwind: GenericStageUnwindFunc(ctx, caCfg,
				NewGenericIndexerUnwinder(
					kv.OtsERC1155,
					kv.OtsERC1155Counter,
					roaring64.BitmapOf(kv.ADDR_ATTR_ERC1155),
				)),
			Prune: NoopStagePrune(ctx, caCfg),
		},
		{
			ID:          stages.OtsERC1167Indexer,
			Description: "ERC1167 proxy indexer",
			Forward: GenericStageForwardFunc(ctx, caCfg, stages.OtsContractIndexer,
				NewConcurrentIndexerExecutor(
					NewERC1167Prober,
					kv.OtsAllContracts,
					kv.OtsERC1167,
					kv.OtsERC1167Counter,
				)),
			Unwind: GenericStageUnwindFunc(ctx, caCfg,
				NewGenericIndexerUnwinder(
					kv.OtsERC1167,
					kv.OtsERC1167Counter,
					roaring64.BitmapOf(kv.ADDR_ATTR_ERC1167),
				)),
			Prune: NoopStagePrune(ctx, caCfg),
		},
		{
			ID:          stages.OtsERC4626Indexer,
			Description: "ERC4626 token indexer",
			Forward: GenericStageForwardFunc(ctx, caCfg, stages.OtsERC20Indexer,
				NewConcurrentIndexerExecutor(
					NewERC4626Prober,
					kv.OtsERC20,
					kv.OtsERC4626,
					kv.OtsERC4626Counter,
				)),
			Unwind: GenericStageUnwindFunc(ctx, caCfg,
				NewGenericIndexerUnwinder(
					kv.OtsERC4626,
					kv.OtsERC4626Counter,
					roaring64.BitmapOf(kv.ADDR_ATTR_ERC4626),
				)),
			Prune: NoopStagePrune(ctx, caCfg),
		},
		{
			ID:          stages.OtsERC20And721Transfers,
			Description: "ERC20/721 token transfer indexer",
			// Binds itself to ERC721 contract classifier as the parent stage on purpose to ensure
			// both ERC20 and ERC721 stages are executed.
			Forward: GenericStageForwardFunc(ctx, caCfg, stages.OtsERC721Indexer, ERC20And721TransferIndexerExecutor),
			Unwind:  GenericStageUnwindFunc(ctx, caCfg, NewGenericLogIndexerUnwinder()),
			Prune:   NoopStagePrune(ctx, caCfg),
		},
		{
			ID:          stages.OtsERC20And721Holdings,
			Description: "ERC20/721 token holdings indexer",
			Forward:     GenericStageForwardFunc(ctx, caCfg, stages.OtsERC721Indexer, ERC20And721HolderIndexerExecutor),
			Unwind:      GenericStageUnwindFunc(ctx, caCfg, NewGenericLogHoldingsUnwinder()),
			Prune:       NoopStagePrune(ctx, caCfg),
		},
		{
			ID:          stages.OtsBlocksRewarded,
			Description: "Blocks rewarded indexer",
			Forward:     GenericStageForwardFunc(ctx, caCfg, stages.Bodies, BlocksRewardedExecutor),
			Unwind:      GenericStageUnwindFunc(ctx, caCfg, NewGenericBlockIndexerUnwinder(kv.OtsBlocksRewardedIndex, kv.OtsBlocksRewardedCounter, RunBlocksRewardedBlockUnwind)),
			Prune:       NoopStagePrune(ctx, caCfg),
		},
		{
			ID:          stages.OtsWithdrawals,
			Description: "CL withdrawals indexer",
			Forward:     GenericStageForwardFunc(ctx, caCfg, stages.Bodies, WithdrawalsExecutor),
			Unwind:      GenericStageUnwindFunc(ctx, caCfg, NewGenericBlockIndexerUnwinder(kv.OtsWithdrawalsIndex, kv.OtsWithdrawalsCounter, RunWithdrawalsBlockUnwind)),
			Prune:       NoopStagePrune(ctx, caCfg),
		},
	}
}
