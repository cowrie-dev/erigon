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

type ERC20Prober struct {
	abi      *abi.ABI
	name     *[]byte
	symbol   *[]byte
	decimals *[]byte
	junkABI  *abi.ABI
	junk     *[]byte
}

func NewERC20Prober() (Prober, error) {
	// ERC20
	aERC20, err := abi.JSON(bytes.NewReader(otscontracts.ERC20))
	if err != nil {
		return nil, err
	}

	// Caches name()/symbol()/decimals() packed calls since they don't require
	// params
	name, err := aERC20.Pack("name")
	if err != nil {
		return nil, err
	}

	symbol, err := aERC20.Pack("symbol")
	if err != nil {
		return nil, err
	}

	decimals, err := aERC20.Pack("decimals")
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

	return &ERC20Prober{
		abi:      &aERC20,
		name:     &name,
		symbol:   &symbol,
		decimals: &decimals,
		junkABI:  &junkABI,
		junk:     &junk,
	}, nil
}

func (p *ERC20Prober) Probe(ctx context.Context, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, blockNum uint64, addr common.Address, _, _ []byte) (*roaring64.Bitmap, error) {
	// decimals()
	res, err, retDecimals := probeContractWithArgs2(ctx, evm, header, chainConfig, ibs, addr, p.abi, p.decimals, "decimals")
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}

	// name()
	res, err, retName := probeContractWithArgs2(ctx, evm, header, chainConfig, ibs, addr, p.abi, p.name, "name")
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}

	// symbol()
	res, err, retSymbol := probeContractWithArgs2(ctx, evm, header, chainConfig, ibs, addr, p.abi, p.symbol, "symbol")
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
	if bytes.Equal(retJunk.ReturnData, retName.ReturnData) &&
		bytes.Equal(retJunk.ReturnData, retSymbol.ReturnData) &&
		bytes.Equal(retJunk.ReturnData, retDecimals.ReturnData) {
		return nil, nil
	}

	return roaring64.BitmapOf(kv.ADDR_ATTR_ERC20), nil
}
