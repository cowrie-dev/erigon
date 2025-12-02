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
	"encoding/binary"
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/erigontech/erigon/common"
	libcommon "github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
)

// TODO(ots2-rebase): PlainState type was removed
type PlainStateStub interface{}

// This executor indexes contract creation.
//
// It produces as result the bucket OtsDeployments with:
//
//   - key: blockNum (uint64)
//   - value: address ([20]byte) + incarnation (uint64)
//
// This bucket serves as a starting point to all contract classifiers.
//
// It follows 2 different strategies, the first one "firstSyncStrategy" is optimized
// for traversing the entire DB and create the entire bucket from existing data. But it works
// only the first time.
//
// The next runs use the "continuousStrategy", which for each block traverses the account changeset
// to detect new contract deployments.
func ContractIndexerExecutor(ctx context.Context, db kv.RoDB, tx kv.RwTx, isInternalTx bool, tmpDir string, chainConfig *chain.Config, blockReader services.FullBlockReader, engine rules.Engine, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, s *StageState, logger log.Logger) (uint64, error) {
	if startBlock == 0 && isInternalTx {
		return firstSyncContractExecutor(tx, tmpDir, chainConfig, blockReader, engine, startBlock, endBlock, isShortInterval, logEvery, ctx, s, logger)
	}
	return incrementalContractIndexer(tx, tmpDir, chainConfig, blockReader, engine, startBlock, endBlock, isShortInterval, logEvery, ctx, s, logger)
}

// This strategy must be run ONLY the first time the contract indexer is run. That's because
// this stage traverses PlainContractCode bucket and indexes all existing addresses that contain
// a deployed contract.
//
// That works only the first time, because it is not unwind friendly, however it's more efficient
// than the general strategy of traversing account change sets.
func firstSyncContractExecutor(tx kv.RwTx, tmpDir string, chainConfig *chain.Config, blockReader services.FullBlockReader, engine rules.Engine, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, ctx context.Context, s *StageState, logger log.Logger) (uint64, error) {
	// TODO(ots2-rebase): kv.PlainContractCode table removed. Need to use domain API to read contracts.
	return startBlock, fmt.Errorf("ContractIndexer not yet ported - kv.PlainContractCode removed")
}

// Helper function to create key/value pairs for contract deployment matches
// blockNum uint64 -> address [20]byte + [incarnation uint64 (ONLY if != 1)]
func contractMatchKeyPair(blockNum uint64, addr libcommon.Address, incarnation uint64) ([]byte, []byte) {
	k := hexutil.EncodeTs(blockNum)

	var v []byte
	if incarnation != 1 {
		v = make([]byte, length.Addr+common.IncarnationLength)
		copy(v, addr.Bytes())
		binary.BigEndian.PutUint64(v[length.Addr:], incarnation)
	} else {
		v = make([]byte, length.Addr)
		copy(v, addr.Bytes())
	}

	return k, v
}

// Given a shard of accounts history, locate which block the incarnation was deployed
// by linear searching all touched blocks and comparing the state before/after each one.
//
// TODO: return DB inconsistency error if can't find block inside shard
func findBlockInsideShard(reader PlainStateStub, bm *roaring64.Bitmap, addr libcommon.Address, incarnation uint64) (uint64, error) {
	// TODO(ots2-rebase): PlainState API completely removed, this function needs full rewrite
	return 0, fmt.Errorf("findBlockInsideShard not yet ported to Erigon 3")
}

func incrementalContractIndexer(tx kv.RwTx, tmpDir string, chainConfig *chain.Config, blockReader services.FullBlockReader, engine rules.Engine, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, ctx context.Context, s *StageState, logger log.Logger) (uint64, error) {
	// TODO(ots2-rebase): kv.AccountChangeSet table removed. Need to use domain API for changesets.
	return startBlock, fmt.Errorf("incrementalContractIndexer not yet ported - kv.AccountChangeSet removed")
}
