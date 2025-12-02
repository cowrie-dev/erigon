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

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
)

// Handles ERC20 and ERC721 indexing simultaneously
type MultiLogIndexerHandler[T any] struct {
	handlers []LogIndexerHandler[T]
}

func NewMultiIndexerHandler[T any](handlers ...LogIndexerHandler[T]) *MultiLogIndexerHandler[T] {
	return &MultiLogIndexerHandler[T]{
		handlers,
	}
}

func (c *MultiLogIndexerHandler[T]) HandleMatch(output *TxMatchedLogs[T]) {
	for _, h := range c.handlers {
		h.HandleMatch(output)
	}
}

func (c *MultiLogIndexerHandler[T]) Flush(force bool) error {
	for _, h := range c.handlers {
		if err := h.Flush(force); err != nil {
			return err
		}
	}
	return nil
}

func (c *MultiLogIndexerHandler[T]) Load(ctx context.Context, tx kv.RwTx) error {
	for _, h := range c.handlers {
		if err := h.Load(ctx, tx); err != nil {
			return err
		}
	}
	return nil
}

func (c *MultiLogIndexerHandler[T]) Close() {
	for _, h := range c.handlers {
		h.Close()
	}
}

// Implements LogIndexerHandler interface in order to index token transfers
// (ERC20/ERC721)
type TransferLogIndexerHandler struct {
	IndexHandler
	nft bool
}

func NewTransferLogIndexerHandler(tmpDir string, s *StageState, nft bool, indexBucket, counterBucket string, logger log.Logger) LogIndexerHandler[TransferAnalysisResult] {
	collector := etl.NewCollector(s.LogPrefix(), tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	bitmaps := map[string]*roaring64.Bitmap{}

	return &TransferLogIndexerHandler{
		&StandardIndexHandler{indexBucket, counterBucket, collector, bitmaps},
		nft,
	}
}

// Add log's ethTx index to from/to addresses indexes
func (h *TransferLogIndexerHandler) HandleMatch(match *TxMatchedLogs[TransferAnalysisResult]) {
	for _, res := range match.matchResults {
		if res.nft != h.nft {
			continue
		}

		// Register this ethTx into from/to transfer addresses indexes
		h.TouchIndex(res.from, match.ethTx)
		h.TouchIndex(res.to, match.ethTx)
	}
}
