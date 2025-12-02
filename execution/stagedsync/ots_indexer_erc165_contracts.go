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

// This is a Prober that detects if an address contains a contract which implements ERC165 interface.
//
// It follows the detection mechanism described in the official specification: https://eips.ethereum.org/EIPS/eip-165
type ERC165Prober struct {
	abi                       *abi.ABI
	supportsInterface165      *[]byte
	supportsInterfaceFFFFFFFF *[]byte
}

func NewERC165Prober() (Prober, error) {
	// ERC165
	aERC165, err := abi.JSON(bytes.NewReader(otscontracts.ERC165))
	if err != nil {
		return nil, err
	}

	// Caches predefined supportsInterface() packed calls
	siEIP165, err := aERC165.Pack("supportsInterface", [4]byte{0x01, 0xff, 0xc9, 0xa7})
	if err != nil {
		return nil, err
	}
	siFFFFFFFF, err := aERC165.Pack("supportsInterface", [4]byte{0xff, 0xff, 0xff, 0xff})
	if err != nil {
		return nil, err
	}

	return &ERC165Prober{
		abi:                       &aERC165,
		supportsInterface165:      &siEIP165,
		supportsInterfaceFFFFFFFF: &siFFFFFFFF,
	}, nil
}

func (p *ERC165Prober) Probe(ctx context.Context, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, blockNum uint64, addr common.Address, _, _ []byte) (*roaring64.Bitmap, error) {
	// supportsInterface(0x01ffc9a7) -> EIP165 interface
	res, err := probeContractWithArgs(ctx, evm, header, chainConfig, ibs, addr, p.abi, p.supportsInterface165, "supportsInterface")
	if err != nil {
		return nil, err
	}
	if res == nil || !res[0].(bool) {
		return nil, nil
	}

	// supportsInterface(0xffffffff) -> MUST return false according to EIP165
	res, err = probeContractWithArgs(ctx, evm, header, chainConfig, ibs, addr, p.abi, p.supportsInterfaceFFFFFFFF, "supportsInterface")
	if err != nil {
		return nil, err
	}
	if res == nil || res[0].(bool) {
		return nil, nil
	}

	return roaring64.BitmapOf(kv.ADDR_ATTR_ERC165), nil
}
