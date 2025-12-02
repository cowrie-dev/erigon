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

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/abi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/stagedsync/otscontracts"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
)

// This is a Prober that detects if an address contains a contract which implements ERC4626 interface.
//
// It assumes ERC20 detection was already done and it passes the criteria.
type ERC4626Prober struct {
	abi         *abi.ABI
	asset       *[]byte
	totalAssets *[]byte
	junkABI     *abi.ABI
	junk        *[]byte
}

func NewERC4626Prober() (Prober, error) {
	// ERC4626
	aIERC4626, err := abi.JSON(bytes.NewReader(otscontracts.IERC4626))
	if err != nil {
		return nil, err
	}

	// Caches asset()/totalAssets() packed calls since they don't require
	// params
	asset, err := aIERC4626.Pack("asset")
	if err != nil {
		return nil, err
	}

	totalAssets, err := aIERC4626.Pack("totalAssets")
	if err != nil {
		return nil, err
	}

	// Junk prober
	junkABI, err := abi.JSON(bytes.NewReader(otscontracts.Junk))
	if err != nil {
		return nil, err
	}
	junk, err := junkABI.Pack("junkjunkjunk")
	if err != nil {
		return nil, err
	}

	return &ERC4626Prober{
		abi:         &aIERC4626,
		asset:       &asset,
		totalAssets: &totalAssets,
		junkABI:     &junkABI,
		junk:        &junk,
	}, nil
}

func (p *ERC4626Prober) Probe(ctx context.Context, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, blockNum uint64, addr common.Address, _, _ []byte) (*roaring64.Bitmap, error) {
	// asset()
	res, err, retAsset := probeContractWithArgs2(ctx, evm, header, chainConfig, ibs, addr, p.abi, p.asset, "asset")
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}

	// totalAssets()
	res, err, retTotalAssets := probeContractWithArgs2(ctx, evm, header, chainConfig, ibs, addr, p.abi, p.totalAssets, "totalAssets")
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}

	// junk
	_, err, retJunk := expectRevert(ctx, evm, header, chainConfig, ibs, &addr, p.junk)
	if err != nil {
		return nil, err
	}

	// Detect faulty contracts that return the same junk raw value no matter what you call;
	// in this case call a random signature and check if it returns the same as name/symbol/decimals,
	// which makes no sense.
	if !retJunk.Failed() && bytes.Equal(retJunk.ReturnData, retAsset.ReturnData) &&
		bytes.Equal(retJunk.ReturnData, retTotalAssets.ReturnData) {
		return nil, nil
	}

	return roaring64.BitmapOf(kv.ADDR_ATTR_ERC4626), nil
}
