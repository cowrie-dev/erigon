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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
)

func (api *Otterscan2APIImpl) GetERC20TransferList(ctx context.Context, addr common.Address, idx, count uint64) (*TransactionListResult, error) {
	return api.genericTransferList(ctx, addr, idx, count, kv.OtsERC20TransferIndex, kv.OtsERC20TransferCounter)
}

func (api *Otterscan2APIImpl) GetERC20TransferCount(ctx context.Context, addr common.Address) (uint64, error) {
	return api.genericGetCount(ctx, addr, kv.OtsERC20TransferCounter)
}

func (api *Otterscan2APIImpl) GetERC721TransferList(ctx context.Context, addr common.Address, idx, count uint64) (*TransactionListResult, error) {
	return api.genericTransferList(ctx, addr, idx, count, kv.OtsERC721TransferIndex, kv.OtsERC721TransferCounter)
}

func (api *Otterscan2APIImpl) GetERC721TransferCount(ctx context.Context, addr common.Address) (uint64, error) {
	return api.genericGetCount(ctx, addr, kv.OtsERC721TransferCounter)
}
