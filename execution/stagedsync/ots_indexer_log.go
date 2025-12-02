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

	roaring "github.com/RoaringBitmap/roaring/v2"
	"github.com/erigontech/erigon/common/cbor"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/types"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
)

// Given a log entry, answer the question: does the ETH tx it belongs to deserves to be indexed?
//
// The generic parameter T represents the analysis result and is implementation-specific. E.g. it
// can contain which addresses this log entry touches in a token transfer indexer.
//
// An instance of this interface is meant to be reused, so it can contain caches to speed up further
// analysis.
type LogAnalyzer[T any] interface {
	// Given a log entry (there may be others in the same tx, here we analyze 1 specific log entry),
	// does it match the criteria the implementation is suposed to analyze?
	//
	// Return nil means it doesn't pass the criteria and it shouldn't be indexed.
	Inspect(tx kv.Tx, l *types.Log) (*T, error)
}

// Handles log indexer lifecycle.
type LogIndexerHandler[T any] interface {
	ResourceAwareIndexHandler

	// Given a tx that must be indexed, handles all logs that caused the matching.
	HandleMatch(match *TxMatchedLogs[T])
}

func runConcurrentLogIndexerExecutor[T any](db kv.RoDB, tx kv.RwTx, blockReader services.FullBlockReader, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, ctx context.Context, s *StageState, analyzer LogAnalyzer[T], handler LogIndexerHandler[T]) (uint64, error) {
	if !isShortInterval {
		log.Info(fmt.Sprintf("[%s] Using concurrent executor", s.LogPrefix()))
	}

	// TODO(ots2-rebase): kv.Log table was removed in Erigon 3. Logs are now in ReceiptDomain.
	// This function needs complete rewrite to use new log reading API.
	return startBlock, fmt.Errorf("LogIndexer not yet ported to Erigon 3 - kv.Log table removed")
}

func runIncrementalLogIndexerExecutor[T any](db kv.RoDB, tx kv.RwTx, blockReader services.FullBlockReader, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, ctx context.Context, s *StageState, analyzer LogAnalyzer[T], handler LogIndexerHandler[T]) (uint64, error) {
	// TODO(ots2-rebase): kv.Log table was removed in Erigon 3. Logs are now in ReceiptDomain.
	// This function needs complete rewrite to use new log reading API.
	return startBlock, fmt.Errorf("LogIndexer not yet ported to Erigon 3 - kv.Log table removed")
}

// Gets a bitmap with all blocks [startBlock, endBlock] that contains at least 1 occurrence of
// a topic (== log0).
//
// The returned bitmap MUST be returned to pool after use.
func newBlockBitmapFromTopic(tx kv.Tx, startBlock, endBlock uint64, topic []byte) (*roaring.Bitmap, error) {
	// TODO(ots2-rebase): kv.LogTopicIndex table was removed. Need to use TblLogTopicsIdx API.
	return nil, fmt.Errorf("newBlockBitmapFromTopic not yet ported to Erigon 3")
}

// Represents a set of all raw logs of 1 transaction that'll be analyzed.
//
// 0 or more logs can contribute for matching and eventual indexing of this
// tx on 0 or more target indexes.
type TxLogs[T any] struct {
	blockNum uint64
	ethTx    uint64
	// raw logs for 1 tx
	rawLogs []byte
}

// k, v are the raw key/value from kv.Logs bucket.
func newTxLogsFromRaw[T any](blockNum, baseTxId uint64, k, v []byte) *TxLogs[T] {
	// idx inside block
	txIdx := binary.BigEndian.Uint32(k[length.BlockNum:])

	// TODO: extract formula function
	ethTx := baseTxId + 1 + uint64(txIdx)

	raw := make([]byte, len(v))
	copy(raw, v)

	return &TxLogs[T]{blockNum, ethTx, raw}
}

// rawLogs contains N encoded logs for 1 tx
func AnalyzeLogs[T any](tx kv.Tx, analyzer LogAnalyzer[T], rawLogs []byte) ([]*T, error) {
	var logs types.Logs
	if err := cbor.Unmarshal(&logs, bytes.NewReader(rawLogs)); err != nil {
		return nil, err
	}

	// scan log entries for tx
	results := make([]*T, 0)
	for _, l := range logs {
		res, err := analyzer.Inspect(tx, l)
		if err != nil {
			return nil, err
		}
		if res == nil {
			continue
		}
		// TODO: dedup
		results = append(results, res)
	}

	return results, nil
}

type TxMatchedLogs[T any] struct {
	*TxLogs[T]
	matchResults []*T
}

func createLogAnalyzerWorker[T any](g *errgroup.Group, ctx context.Context, db kv.RoDB, analyzer LogAnalyzer[T], proberCh <-chan *TxLogs[T], matchCh chan<- *TxMatchedLogs[T], totalMatch, txCount *atomic.Uint64) {
	g.Go(func() error {
		return db.View(ctx, func(tx kv.Tx) error {
			for {
				// wait for input
				txLogs, ok := <-proberCh
				if !ok {
					break
				}

				results, err := AnalyzeLogs(tx, analyzer, txLogs.rawLogs)
				if err != nil {
					return err
				}

				txCount.Inc()
				if len(results) > 0 {
					totalMatch.Inc()
					matchCh <- &TxMatchedLogs[T]{txLogs, results}
				}
			}
			return nil
		})
	})
}
