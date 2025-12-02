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

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/abi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/holiman/uint256"
)

// A Prober evaluates the contents of a certain address and determines if it passes or fails the test
// it is programmed to.
//
// What/how it tests is determined by the implementation and is out of scope of this interface.
// We only care about yes/no/error.
//
// Users of this interface can use the result of the test to decide this address belongs to a certain
// class of content, i.e., someone could use an ERC20 prober implementation to detect ERC20 token
// contracts and index them in the DB.
type Prober interface {
	// Given an EVM context passed as parameters to this function, return if it passes the test or it errored.
	//
	// addr is the ETH address which is being analyzed. Contract probers will usually target this address
	// and staticcall functions to determine if it complies to what is being probed.
	//
	// sourceK/sourceV are the original key/value from the source bucket. Probers will usually ignore
	// them unless it is an advanced prober that is aware of the source bucket and wants to do a in-depth
	// check.
	//
	// It the returned attrs bitmap is nil, it means the address does not match any of the characteristics
	// this prober implementation is programmed to analyze. Otherwise it returns a bitmap of attributes.
	Probe(ctx context.Context, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, blockNum uint64, addr common.Address, sourceK, sourceV []byte) (attrs *roaring64.Bitmap, err error)
}

// Creates a Prober instance
type ProberFactory func() (Prober, error)

// TODO: remove and rename the "2" variant
func probeContractWithArgs(ctx context.Context, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, addr common.Address, abi *abi.ABI, data *[]byte, outputName string) ([]interface{}, error) {
	// Use block gas limit for the call
	gas := hexutil.Uint64(header.GasLimit)
	args := ethapi.CallArgs{
		To:   &addr,
		Data: (*hexutil.Bytes)(data),
		Gas:  &gas,
	}

	ret, err := probeContract(ctx, evm, header, chainConfig, ibs, args)
	if err != nil {
		return nil, err
	}
	if ret.Err != nil {
		// ignore errors because we are probing untrusted contract
		return nil, nil
	}
	res, err := abi.Unpack(outputName, ret.ReturnData)
	if err != nil {
		// ignore errors because we are probing untrusted contract
		return nil, nil
	}

	return res, nil
}

func probeContractWithArgs2(ctx context.Context, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, addr common.Address, abi *abi.ABI, data *[]byte, outputName string) ([]interface{}, error, *evmtypes.ExecutionResult) {
	// Use block gas limit for the call
	gas := hexutil.Uint64(header.GasLimit)
	args := ethapi.CallArgs{
		To:   &addr,
		Data: (*hexutil.Bytes)(data),
		Gas:  &gas,
	}

	ret, err := probeContract(ctx, evm, header, chainConfig, ibs, args)
	if err != nil {
		return nil, err, nil
	}
	if ret.Failed() {
		// ignore errors because we are probing untrusted contract
		return nil, nil, nil
	}
	res, err := abi.Unpack(outputName, ret.ReturnData)
	if err != nil {
		// ignore errors because we are probing untrusted contract
		return nil, nil, nil
	}

	return res, nil, ret
}

func expectRevert(ctx context.Context, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, addr *common.Address, data *[]byte) (bool, error, *evmtypes.ExecutionResult) {
	gas := hexutil.Uint64(header.GasLimit)
	args := ethapi.CallArgs{
		To:   addr,
		Data: (*hexutil.Bytes)(data),
		Gas:  &gas,
	}
	ret, err := probeContract(ctx, evm, header, chainConfig, ibs, args)
	if err != nil {
		// internal error
		return false, err, nil
	}

	return ret.Failed(), nil, ret
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
	// TODO(ots2-rebase): ApplyMessage now requires rules.Engine, passing nil for now
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
