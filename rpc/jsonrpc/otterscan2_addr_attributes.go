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
	"bytes"
	"context"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/bitmapdb"
)

type AddrAttributes struct {
	ERC20   bool `json:"erc20,omitempty"`
	ERC165  bool `json:"erc165,omitempty"`
	ERC721  bool `json:"erc721,omitempty"`
	ERC1155 bool `json:"erc1155,omitempty"`
	ERC1167 bool `json:"erc1167,omitempty"`
}

func (api *Otterscan2APIImpl) GetAddressAttributes(ctx context.Context, addr common.Address) (*AddrAttributes, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	v, err := tx.GetOne(kv.OtsAddrAttributes, addr.Bytes())
	if err != nil {
		return nil, err
	}
	if v == nil {
		return &AddrAttributes{}, nil
	}

	bm := bitmapdb.NewBitmap64()
	defer bitmapdb.ReturnToPool64(bm)
	if _, err := bm.ReadFrom(bytes.NewReader(v)); err != nil {
		return nil, err
	}

	attr := AddrAttributes{
		ERC20:   bm.Contains(kv.ADDR_ATTR_ERC20),
		ERC165:  bm.Contains(kv.ADDR_ATTR_ERC165),
		ERC721:  bm.Contains(kv.ADDR_ATTR_ERC721),
		ERC1155: bm.Contains(kv.ADDR_ATTR_ERC1155),
		ERC1167: bm.Contains(kv.ADDR_ATTR_ERC1167),
	}
	return &attr, nil
}
