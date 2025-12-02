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
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
)

// This is a Prober that detects if an address contains a contract which implements an ERC1167 minimal proxy
// contract.
//
// It matches the bytecode describe in the specification: https://eips.ethereum.org/EIPS/eip-1167
type ERC1167Prober struct {
}

func NewERC1167Prober() (Prober, error) {
	return &ERC1167Prober{}, nil
}

var minimalProxyTemplate = hexutil.MustDecode("0x363d3d373d3d3d363d73bebebebebebebebebebebebebebebebebebebebe5af43d82803e903d91602b57fd5bf3")

// TODO: implement support for ERC1167 push optimizations
func (i *ERC1167Prober) Probe(ctx context.Context, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, blockNum uint64, addr common.Address, _, _ []byte) (*roaring64.Bitmap, error) {
	code, err := ibs.GetCode(addr)
	if err != nil {
		return nil, err
	}
	if len(code) != len(minimalProxyTemplate) {
		return nil, nil
	}

	if !bytes.HasPrefix(code, minimalProxyTemplate[:10]) {
		return nil, nil
	}
	if !bytes.HasSuffix(code, minimalProxyTemplate[30:]) {
		return nil, nil
	}

	return roaring64.BitmapOf(kv.ADDR_ATTR_ERC1167), nil
}
