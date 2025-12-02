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
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/abi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/holiman/uint256"
)

type Otterscan2API interface {
	GetAllContractsList(ctx context.Context, idx, count uint64) (*ContractListResult, error)
	GetAllContractsCount(ctx context.Context) (uint64, error)
	GetERC20List(ctx context.Context, idx, count uint64) (*ContractListResult, error)
	GetERC20Count(ctx context.Context) (uint64, error)
	GetERC721List(ctx context.Context, idx, count uint64) (*ContractListResult, error)
	GetERC721Count(ctx context.Context) (uint64, error)
	GetERC1155List(ctx context.Context, idx, count uint64) (*ContractListResult, error)
	GetERC1155Count(ctx context.Context) (uint64, error)
	GetERC1167List(ctx context.Context, idx, count uint64) (*ContractListResult, error)
	GetERC1167Count(ctx context.Context) (uint64, error)
	GetERC4626List(ctx context.Context, idx, count uint64) (*ContractListResult, error)
	GetERC4626Count(ctx context.Context) (uint64, error)

	GetERC1167Impl(ctx context.Context, addr common.Address) (common.Address, error)

	GetAddressAttributes(ctx context.Context, addr common.Address) (*AddrAttributes, error)

	GetERC20TransferList(ctx context.Context, addr common.Address, idx, count uint64) (*TransactionListResult, error)
	GetERC20TransferCount(ctx context.Context, addr common.Address) (uint64, error)
	GetERC721TransferList(ctx context.Context, addr common.Address, idx, count uint64) (*TransactionListResult, error)
	GetERC721TransferCount(ctx context.Context, addr common.Address) (uint64, error)
	GetERC20Holdings(ctx context.Context, addr common.Address) ([]*HoldingMatch, error)
	GetERC721Holdings(ctx context.Context, addr common.Address) ([]*HoldingMatch, error)

	GetBlocksRewardedList(ctx context.Context, addr common.Address, idx, count uint64) (*BlocksRewardedListResult, error)
	GetBlocksRewardedCount(ctx context.Context, addr common.Address) (uint64, error)
	GetWithdrawalsList(ctx context.Context, addr common.Address, idx, count uint64) (*WithdrawalsListResult, error)
	GetWithdrawalsCount(ctx context.Context, addr common.Address) (uint64, error)

	TransferIntegrityChecker(ctx context.Context) error
	HoldingsIntegrityChecker(ctx context.Context) error
}

type Otterscan2APIImpl struct {
	*BaseAPI
	db kv.RoDB
}

func NewOtterscan2API(base *BaseAPI, db kv.RoDB) *Otterscan2APIImpl {
	return &Otterscan2APIImpl{
		BaseAPI: base,
		db:      db,
	}
}

// Max results that can be requested by genericMatchingList callers to avoid node DoS
const MAX_MATCH_COUNT = uint64(500)

// TODO: replace by BlockSummary2
type BlockSummary struct {
	Block hexutil.Uint64 `json:"blockNumber"`
	Time  uint64         `json:"timestamp"`
}

type BlockSummary2 struct {
	Block hexutil.Uint64 `json:"blockNumber"`
	Time  uint64         `json:"timestamp"`
	internalIssuance
	TotalFees *hexutil.Big `json:"totalFees"`
}

type AddrMatch struct {
	Block   *hexutil.Uint64 `json:"blockNumber"`
	Address *common.Address `json:"address"`
}

func ToBlockSlice(addrMatches []AddrMatch) []hexutil.Uint64 {
	res := make([]hexutil.Uint64, 0, len(addrMatches))
	for _, m := range addrMatches {
		res = append(res, *m.Block)
	}
	return res
}

func (api *Otterscan2APIImpl) newBlocksSummaryFromResults(ctx context.Context, tx kv.Tx, res []hexutil.Uint64) (map[hexutil.Uint64]*BlockSummary, error) {
	ret := make(map[hexutil.Uint64]*BlockSummary, 0)

	for _, m := range res {
		if _, ok := ret[m]; ok {
			continue
		}

		header, err := api._blockReader.HeaderByNumber(ctx, tx, uint64(m))
		if err != nil {
			return nil, err
		}

		ret[m] = &BlockSummary{m, header.Time}
	}

	return ret, nil
}

func (api *Otterscan2APIImpl) newBlocksSummary2FromResults(ctx context.Context, tx kv.Tx, res []hexutil.Uint64) (map[hexutil.Uint64]*BlockSummary2, error) {
	ret := make(map[hexutil.Uint64]*BlockSummary2, 0)

	for _, m := range res {
		if _, ok := ret[m]; ok {
			continue
		}

		number := rpc.BlockNumber(m)
		b, senders, err := api.getBlockWithSenders(ctx, number, tx)
		if err != nil {
			return nil, err
		}
		if b == nil {
			return nil, nil
		}
		details, err := api.getBlockDetailsImpl(ctx, tx, b, number, senders)
		if err != nil {
			return nil, err
		}

		ret[m] = details
	}

	return ret, nil
}

// Given an address search match, extract some extra data by running EVM against it
type ExtraDataExtractor func(tx kv.Tx, res *AddrMatch, addr common.Address, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, stateReader state.StateReader) (interface{}, error)

func (api *Otterscan2APIImpl) genericExtraData(ctx context.Context, tx kv.Tx, res []AddrMatch, extraData ExtraDataExtractor) ([]interface{}, error) {
	newRes := make([]interface{}, 0, len(res))

	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	var evm *vm.EVM
	// TODO(ots2-rebase): NewPlainStateReader removed - state reading API changed
	// stateReader := state.NewPlainStateReader(tx)
	// var ibs *state.IntraBlockState
	ibs := state.New(nil) // TODO: need proper state reader
	prevBlock := uint64(0)

	blockReader := api._blockReader
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, e := blockReader.Header(ctx, tx, hash, number)
		if e != nil {
			log.Error("getHeader error", "number", number, "hash", hash, "err", e)
		}
		return h
	}
	engine := api.engine()

	blockNumber, hash, _, err := rpchelper.GetCanonicalBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(rpc.LatestExecutedBlockNumber), tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}
	block, err := api.blockWithSenders(ctx, tx, hash, blockNumber)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	header := block.HeaderNoCopy()

	for _, r := range res {
		// TODO: this is failing when block is the tip, hence block + 1 header can't be found!
		/////////////////////
		originalBlockNum := uint64(*r.Block + 1)

		// var header *types.Header
		// header, err = blockReader.HeaderByNumber(ctx, tx, originalBlockNum)
		// if err != nil {
		// 	return nil, err
		// }
		if header == nil {
			return nil, fmt.Errorf("couldn't find header for block %d", originalBlockNum)
		}

		// TODO(ots2-rebase): All state reading code commented out - NewPlainState/NewPlainStateReader removed
		// if stateReader == nil {
		// 	stateReader = state.NewPlainStateReader(tx)
		// 	ibs = state.New(stateReader)
		// } else if originalBlockNum != prevBlock {
		// 	stateReader.SetBlockNr(originalBlockNum + 1)
		// 	ibs.Reset()
		// }

		if evm == nil {
			getHashFn := func(n uint64) (common.Hash, error) {
				h := getHeader(common.Hash{}, n)
				if h == nil {
					return common.Hash{}, nil
				}
				return h.Hash(), nil
			}
			blockCtx := protocol.NewEVMBlockContext(header, getHashFn, engine, nil /* author */, chainConfig)
			evm = vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs, chainConfig, vm.Config{NoBaseFee: true})
		} else {
			if originalBlockNum != prevBlock {
				// reset block
				getHashFn := func(n uint64) (common.Hash, error) {
					h := getHeader(common.Hash{}, n)
					if h == nil {
						return common.Hash{}, nil
					}
					return h.Hash(), nil
				}
				blockCtx := protocol.NewEVMBlockContext(header, getHashFn, engine, nil /* author */, chainConfig)
				// TODO(ots2-rebase): ResetBetweenBlocks now requires *chain.Rules parameter
				// Using nil for now - may cause runtime issues
				evm.ResetBetweenBlocks(blockCtx, evmtypes.TxContext{}, ibs, vm.Config{NoBaseFee: true}, nil)
			}
		}
		prevBlock = originalBlockNum
		/////////////////////

		addr := r.Address
		ibs.Reset()
		extra, err := extraData(tx, &r, *addr, evm, header, chainConfig, ibs, nil /* stateReader */)
		if err != nil {
			return nil, err
		}
		newRes = append(newRes, extra)

		select {
		default:
		case <-ctx.Done():
			return nil, common.ErrStopped
		}
	}

	return newRes, nil
}

func decodeReturnData(ctx context.Context, addr *common.Address, data []byte, methodName string, header *types.Header, evm *vm.EVM, chainConfig *chain.Config, ibs *state.IntraBlockState, contractABI *abi.ABI) (interface{}, error) {
	gas := hexutil.Uint64(header.GasLimit)
	args := ethapi.CallArgs{
		To:   addr,
		Data: (*hexutil.Bytes)(&data),
		Gas:  &gas,
	}
	ret, err := probeContract(ctx, evm, header, chainConfig, ibs, args)
	if err != nil {
		// internal error
		return nil, err
	}

	if ret.Err != nil {
		// ignore on purpose; i.e., out of gas signals error here
		log.Warn(fmt.Sprintf("error while trying to unpack %s: %v", methodName, ret.Err))
		return nil, nil
	}

	retVal, err := contractABI.Unpack(methodName, ret.ReturnData)
	if err != nil {
		// ignore on purpose; untrusted contract doesn't comply to expected ABI
		log.Warn(fmt.Sprintf("error while trying to unpack %s: %v", methodName, err))
		return nil, nil
	}

	return retVal[0], nil
}

func probeContract(ctx context.Context, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, state *state.IntraBlockState, args ethapi.CallArgs) (*evmtypes.ExecutionResult, error) {
	var baseFee *uint256.Int
	if header != nil && header.BaseFee != nil {
		var overflow bool
		baseFee, overflow = uint256.FromBig(header.BaseFee)
		if overflow {
			return nil, fmt.Errorf("header.BaseFee uint256 overflow")
		}
	}
	msg, err := args.ToMessage(0, baseFee)
	if err != nil {
		return nil, err
	}

	txCtx := protocol.NewEVMTxContext(msg)
	state.Reset()
	evm.Reset(txCtx, state)

	gp := new(protocol.GasPool).AddGas(msg.Gas())
	// TODO(ots2-rebase): ApplyMessage now requires rules.Engine parameter
	result, err := protocol.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */, nil)
	if err != nil {
		return nil, err
	}

	// If the timer caused an abort, return an appropriate error message
	if evm.Cancelled() {
		return nil, fmt.Errorf("execution aborted (timeout = )")
	}
	return result, nil
}
