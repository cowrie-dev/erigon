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
	"encoding/binary"
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/bitmapdb"
	"github.com/erigontech/erigon/ots/indexer"
)

const (
	bitmapsBufLimit   = uint64(256 * 1024 * 1024) // 256MB
	bitmapsFlushEvery = 30 * time.Second
)

// TODO(ots2-rebase): needFlush64 was removed. Need to implement proper bitmap size checking.
func needFlush64(bitmaps map[string]*roaring64.Bitmap, limit uint64) bool {
	var size uint64
	for _, bm := range bitmaps {
		size += bm.GetSerializedSizeInBytes()
		if size >= limit {
			return true
		}
	}
	return false
}

// TODO(ots2-rebase): flushBitmaps64 was removed. Need to implement proper bitmap flushing.
func flushBitmaps64(collector *etl.Collector, bitmaps map[string]*roaring64.Bitmap) error {
	for addr, bm := range bitmaps {
		buf := bytes.NewBuffer(nil)
		if _, err := bm.WriteTo(buf); err != nil {
			return err
		}
		if err := collector.Collect([]byte(addr), buf.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

// Standard (in a meaning everyone is meant to use it) implementation of
// IndexHandler
type StandardIndexHandler struct {
	indexBucket   string
	counterBucket string
	collector     *etl.Collector
	bitmaps       map[string]*roaring64.Bitmap
}

func (h *StandardIndexHandler) TouchIndex(addr common.Address, idx uint64) {
	bm, ok := h.bitmaps[string(addr.Bytes())]
	if !ok {
		bm = roaring64.NewBitmap()
		h.bitmaps[string(addr.Bytes())] = bm
	}
	bm.Add(idx)
}

func (h *StandardIndexHandler) Flush(force bool) error {
	if force || needFlush64(h.bitmaps, bitmapsBufLimit) {
		if err := flushBitmaps64(h.collector, h.bitmaps); err != nil {
			return err
		}
		h.bitmaps = map[string]*roaring64.Bitmap{}
	}

	return nil
}

func (h *StandardIndexHandler) Load(ctx context.Context, tx kv.RwTx) error {
	transferCounter, err := tx.RwCursorDupSort(h.counterBucket)
	if err != nil {
		return err
	}
	defer transferCounter.Close()

	buf := bytes.NewBuffer(nil)
	addrBm := roaring64.NewBitmap()

	loadFunc := func(k []byte, value []byte, tableReader etl.CurrentTableReader, next etl.LoadNextFunc) error {
		// Bitmap for address key
		if _, err := addrBm.ReadFrom(bytes.NewBuffer(value)); err != nil {
			return err
		}

		// Last chunk for address key
		addr := k[:length.Addr]

		// Read last chunk from DB (may not exist)
		// Chunk already exists; merge it
		if err := mergeLastChunk(addrBm, addr, tableReader); err != nil {
			return err
		}

		// Recover and delete the last counter (may not exist); will be replaced after this chunk write
		prevCounter := uint64(0)
		isUniqueChunk := false
		counterK, _, err := transferCounter.SeekExact(addr)
		if err != nil {
			return err
		}
		if counterK != nil {
			counterV, err := transferCounter.LastDup()
			if err != nil {
				return err
			}
			if len(counterV) == 1 {
				// Optimized counter; prevCounter must remain 0
				c, err := transferCounter.CountDuplicates()
				if err != nil {
					return err
				}
				if c != 1 {
					return fmt.Errorf("db possibly corrupted: bucket=%s addr=%s has optimized counter with duplicates", h.counterBucket, hexutil.Encode(addr))
				}

				isUniqueChunk = true
			} else {
				// Regular counter
				chunk := counterV[8:]
				chunkAsNumber := binary.BigEndian.Uint64(chunk)
				if chunkAsNumber != ^uint64(0) {
					return fmt.Errorf("db possibly corrupted: bucket=%s addr=%s last chunk is not 0xffffffffffffffff: %s", h.counterBucket, hexutil.Encode(addr), hexutil.Encode(chunk))
				}
			}

			// Delete last counter, optimized or not; it doesn't matter, it'll be
			// rewriten below
			if err := transferCounter.DeleteCurrent(); err != nil {
				return err
			}

			// Regular chunk, rewind to previous counter
			if !isUniqueChunk {
				prevK, prevV, err := transferCounter.PrevDup()
				if err != nil {
					return err
				}
				if prevK != nil {
					prevCounter = binary.BigEndian.Uint64(prevV[:8])
				}
			}
		}

		// Write the index chunk; cut it if necessary to fit under page restrictions
		if (counterK == nil || isUniqueChunk) && prevCounter+addrBm.GetCardinality() <= 256 {
			buf.Reset()
			b := make([]byte, 8)
			for it := addrBm.Iterator(); it.HasNext(); {
				ethTx := it.Next()
				binary.BigEndian.PutUint64(b, ethTx)
				buf.Write(b)
			}

			_, err := h.writeOptimizedChunkAndCounter(tx, k, buf, addr, next, prevCounter)
			if err != nil {
				return err
			}
		} else {
			buf.Reset()
			b := make([]byte, 8)
			for it := addrBm.Iterator(); it.HasNext(); {
				ethTx := it.Next()
				binary.BigEndian.PutUint64(b, ethTx)
				buf.Write(b)

				// cut?
				if !it.HasNext() || buf.Len() >= int(bitmapdb.ChunkLimit) {
					updatedCounter, err := h.writeRegularChunkAndCounter(tx, k, buf, addr, next, ethTx, !it.HasNext(), prevCounter)
					if err != nil {
						return err
					}
					prevCounter = updatedCounter

					// Cleanup buffer for next chunk
					buf.Reset()
				}
			}
		}

		return nil
	}
	if err := h.collector.Load(tx, h.indexBucket, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	return nil
}

func (h *StandardIndexHandler) writeOptimizedChunkAndCounter(tx kv.RwTx, k []byte, buf *bytes.Buffer, addr []byte, next etl.LoadNextFunc, prevCounter uint64) (uint64, error) {
	// Write solo chunk
	chunkKey := chunkKey(k, true, 0)
	if err := next(k, chunkKey, buf.Bytes()); err != nil {
		return 0, err
	}

	// Write optimized counter
	prevCounter += uint64(buf.Len()) / 8
	v := indexer.OptimizedCounterSerializer(prevCounter)
	if err := tx.Put(h.counterBucket, addr, v); err != nil {
		return 0, err
	}

	return prevCounter, nil
}

func (h *StandardIndexHandler) writeRegularChunkAndCounter(tx kv.RwTx, k []byte, buf *bytes.Buffer, addr []byte, next etl.LoadNextFunc, ethTx uint64, isLast bool, prevCounter uint64) (uint64, error) {
	chunkKey := chunkKey(k, isLast, ethTx)
	if err := next(k, chunkKey, buf.Bytes()); err != nil {
		return 0, err
	}

	// Write updated counter
	prevCounter += uint64(buf.Len()) / 8
	v := indexer.RegularCounterSerializer(prevCounter, chunkKey[length.Addr:])
	if err := tx.Put(h.counterBucket, addr, v); err != nil {
		return 0, err
	}

	return prevCounter, nil
}

// Reads the last index chunk for a certain address and merge the result
// into the currently being processed bitmap.
func mergeLastChunk(addrBm *roaring64.Bitmap, addr []byte, tableReader etl.CurrentTableReader) error {
	chunkBm := bitmapdb.NewBitmap64()
	defer bitmapdb.ReturnToPool64(chunkBm)

	key := make([]byte, length.Addr+8)
	copy(key, addr)
	binary.BigEndian.PutUint64(key[length.Addr:], ^uint64(0))

	// Read last chunk from DB (may not exist)
	v, err := tableReader.Get(key)
	if err != nil {
		return err
	}
	if v == nil {
		return nil
	}

	for i := 0; i < len(v); i += 8 {
		chunkBm.Add(binary.BigEndian.Uint64(v[i : i+8]))
	}
	addrBm.Or(chunkBm)

	return nil
}

// k == address [length.Addr]byte + chunk uint64
func chunkKey(k []byte, isLast bool, ethTx uint64) []byte {
	key := make([]byte, length.Addr+8)
	copy(key, k[:length.Addr])

	if isLast {
		binary.BigEndian.PutUint64(key[length.Addr:], ^uint64(0))
	} else {
		binary.BigEndian.PutUint64(key[length.Addr:], ethTx)
	}

	return key
}

func (h *StandardIndexHandler) Close() {
	h.collector.Close()
}
