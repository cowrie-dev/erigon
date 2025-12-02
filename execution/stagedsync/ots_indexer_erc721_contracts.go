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

// This is a Prober that detects if an address contains a contract which implements ERC721 interface.
//
// It assumes ERC165 detection was already done and it passes the criteria.
type ERC721Prober struct {
	abi                    *abi.ABI
	supportsInterface721   *[]byte
	supportsInterface721MD *[]byte
}

// TODO: support 721 and 721MD simultaneously
func NewERC721Prober() (Prober, error) {
	a, err := abi.JSON(bytes.NewReader(otscontracts.ERC165))
	if err != nil {
		return nil, err
	}

	// Caches predefined supportsInterface() packed calls
	siEIP721, err := a.Pack("supportsInterface", [4]byte{0x80, 0xac, 0x58, 0xcd})
	if err != nil {
		return nil, err
	}
	si721MD, err := a.Pack("supportsInterface", [4]byte{0x5b, 0x5e, 0x13, 0x9f})
	if err != nil {
		return nil, err
	}

	return &ERC721Prober{
		abi:                    &a,
		supportsInterface721:   &siEIP721,
		supportsInterface721MD: &si721MD,
	}, nil
}

func (p *ERC721Prober) Probe(ctx context.Context, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, blockNum uint64, addr common.Address, _, _ []byte) (*roaring64.Bitmap, error) {
	bm := roaring64.NewBitmap()

	// supportsInterface(0x80ac58cd) -> ERC721 interface
	res, err := probeContractWithArgs(ctx, evm, header, chainConfig, ibs, addr, p.abi, p.supportsInterface721, "supportsInterface")
	if err != nil {
		return nil, err
	}
	if res == nil || !res[0].(bool) {
		return nil, nil
	}
	bm.Add(kv.ADDR_ATTR_ERC721)

	// supportsInterface(0x5b5e139f) -> ERC721 Metadata
	res, err = probeContractWithArgs(ctx, evm, header, chainConfig, ibs, addr, p.abi, p.supportsInterface721MD, "supportsInterface")
	if err != nil {
		return nil, err
	}
	if res != nil && res[0].(bool) {
		bm.Add(kv.ADDR_ATTR_ERC721) // Fixed: was ADDR_ATTR_ERC721_MD
	}

	return bm, nil
}
