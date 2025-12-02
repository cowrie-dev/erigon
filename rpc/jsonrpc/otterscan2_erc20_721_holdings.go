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
	"encoding/binary"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
)

type HoldingMatch struct {
	Address common.Address `json:"address"`
	Tx      uint64         `json:"ethTx"`
}

func (api *Otterscan2APIImpl) GetERC20Holdings(ctx context.Context, holder common.Address) ([]*HoldingMatch, error) {
	return api.getHoldings(ctx, holder, kv.OtsERC20Holdings)
}

func (api *Otterscan2APIImpl) GetERC721Holdings(ctx context.Context, holder common.Address) ([]*HoldingMatch, error) {
	return api.getHoldings(ctx, holder, kv.OtsERC721Holdings)
}

func (api *Otterscan2APIImpl) getHoldings(ctx context.Context, holder common.Address, holdingsBucket string) ([]*HoldingMatch, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, nil
	}
	defer tx.Rollback()

	holdings, err := tx.CursorDupSort(holdingsBucket)
	if err != nil {
		return nil, err
	}
	defer holdings.Close()

	k, v, err := holdings.SeekExact(holder.Bytes())
	if err != nil {
		return nil, err
	}
	if k == nil {
		return make([]*HoldingMatch, 0), nil
	}

	ret := make([]*HoldingMatch, 0)
	for k != nil {
		token := common.BytesToAddress(v[:length.Addr])
		ethTx := binary.BigEndian.Uint64(v[length.Addr:])

		ret = append(ret, &HoldingMatch{
			Address: token,
			Tx:      ethTx,
		})

		k, v, err = holdings.NextDup()
		if err != nil {
			return nil, err
		}
	}

	return ret, nil
}
