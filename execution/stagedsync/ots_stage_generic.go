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
	"fmt"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

type ContractAnalyzerCfg struct {
	db          kv.RwDB
	tmpDir      string
	chainConfig *chain.Config
	blockReader services.FullBlockReader
	engine      rules.Engine
}

func StageDbAwareCfg(db kv.RwDB, tmpDir string, chainConfig *chain.Config, blockReader services.FullBlockReader, engine rules.Engine) ContractAnalyzerCfg {
	return ContractAnalyzerCfg{
		db,
		tmpDir,
		chainConfig,
		blockReader,
		engine,
	}
}

// This is a hint to supress verbose info logs if the stage block range to be executed is <= this number
// of blocks.
const SHORT_RANGE_EXECUTION_THRESHOLD = 16

// Defines a stage executor function to be called back by GenericStageForwardFunc.
//
// It should implement the stage business logic.
//
// Implementation should rely on the tx param for DB access. The db param is provided for a specific
// use-case and should be used with caution.
//
// The db param should be used by concurrent implementations that are optimized for the first sync, hence
// there is no risk of trying to read uncommited data. In this case, the implementation can span several
// goroutines to process data saved by a previous stages concurrently.
type StageExecutor = func(ctx context.Context, db kv.RoDB, tx kv.RwTx, isInternalTx bool, tmpDir string, chainConfig *chain.Config, blockReader services.FullBlockReader, engine rules.Engine, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, s *StageState, logger log.Logger) (uint64, error)

// Defines a stage executor function to be called back by GenericStageUnwindFunc.
//
// It should implement the stage unwind business logic.
//
// Implementation should rely on the tx param for DB access.
type UnwindExecutor = func(ctx context.Context, tx kv.RwTx, u *UnwindState, blockReader services.FullBlockReader, isShortInterval bool, logEvery *time.Ticker) error

// This is a template factory function of stage forward execution implementation.
//
// This implementation handles most common tasks performed by stage execution and allows custom logic
// to be plugged via an executor param.
//
// Tasks performed by this generic implementation:
//
//   - It handles the lack of parent tx param meaning stage must begin/commit its own db tx.
//   - It provides a standard log ticker.
//   - It determines the block range so verbose logs may be supressed.
//   - It handles execution completion automatically (db save of last successful block).
func GenericStageForwardFunc(ctx context.Context, cfg ContractAnalyzerCfg, parentStage stages.SyncStage, executor StageExecutor) ExecFunc {
	// TODO(ots2-rebase): Stage API changed - now uses SharedDomains + TemporalRwTx instead of TxContainer
	return func(badBlockUnwind bool, s *StageState, u Unwinder, doms *execctx.SharedDomains, rwTx kv.TemporalRwTx, logger log.Logger) error {
		return genericStageForwardImpl(ctx, cfg, s, rwTx, executor, parentStage, logger, false, 0, 0)
	}
}

func GenericStageForwardFuncWithDebug(ctx context.Context, cfg ContractAnalyzerCfg, parentStage stages.SyncStage, executor StageExecutor, _debugStartBlock, _debugEndBlock uint64) ExecFunc {
	// TODO(ots2-rebase): Stage API changed - now uses SharedDomains + TemporalRwTx instead of TxContainer
	return func(badBlockUnwind bool, s *StageState, u Unwinder, doms *execctx.SharedDomains, rwTx kv.TemporalRwTx, logger log.Logger) error {
		if s.BlockNumber > 0 {
			return nil
		}
		return genericStageForwardImpl(ctx, cfg, s, rwTx, executor, parentStage, logger, true, _debugStartBlock, _debugEndBlock)
	}
}

func genericStageForwardImpl(ctx context.Context, cfg ContractAnalyzerCfg, s *StageState, tx kv.RwTx, executor StageExecutor, parentStage stages.SyncStage, logger log.Logger, _debug bool, _debugStartBlock, _debugEndBlock uint64) error {
	useExternalTx := tx != nil

	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	// Determine [startBlock, endBlock]
	//
	// If saved block number == 0, it means stage was never run and it must start at 0, otherwise
	// it represents the latest ran block number, so it must start at block+1.
	//
	// End block is bound to latest run block number from the parent stage. Must not go further.
	startBlock := s.BlockNumber
	if startBlock > 0 {
		startBlock++
	}
	endBlock, err := stages.GetStageProgress(tx, parentStage)
	if err != nil {
		return err
	}

	///////////////////////////
	// DEBUG OVERRIDES
	if _debug {
		startBlock = _debugStartBlock
		endBlock = _debugEndBlock
	}
	///////////////////////////

	// startBlock > endBlock means parent stage progress was forcefully reset
	// just skip this stage silently
	if startBlock > endBlock {
		return nil
	}

	// Don't display verbose start/finish logs on short range executions; given short == <= N blocks
	isShortInterval := endBlock-startBlock+1 <= SHORT_RANGE_EXECUTION_THRESHOLD

	///////////////////////////
	// DEBUG OVERRIDES
	if _debug {
		isShortInterval = false
	}
	///////////////////////////

	if !isShortInterval {
		log.Info(fmt.Sprintf("[%s] Started", s.LogPrefix()), "from", startBlock, "to", endBlock)
	}
	lastFinishedBlock, err := executor(ctx, cfg.db, tx, !useExternalTx, cfg.tmpDir, cfg.chainConfig, cfg.blockReader, cfg.engine, startBlock, endBlock, isShortInterval, logEvery, s, logger)
	if err != nil {
		return err
	}
	if !isShortInterval {
		log.Info(fmt.Sprintf("[%s] Finished", s.LogPrefix()), "latest", lastFinishedBlock)
	}

	if err := s.Update(tx, lastFinishedBlock); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// This is a template factory function of stage Unwind implementation.
//
// This implementation handles most common tasks performed by unwind and allows custom logic
// to be plugged via an executor param.
//
// Tasks performed by this generic implementation:
//
//   - It handles the lack of parent tx param meaning stage must begin/commit its own db tx.
//   - It provides a standard log ticker.
//   - It determines the block range so verbose logs may be supressed.
//   - It handles unwind completion automatically (db saves).
func GenericStageUnwindFunc(ctx context.Context, cfg ContractAnalyzerCfg, executor UnwindExecutor) UnwindFunc {
	// TODO(ots2-rebase): Unwind API changed to use SharedDomains + TemporalRwTx
	return func(u *UnwindState, s *StageState, doms *execctx.SharedDomains, rwTx kv.TemporalRwTx, logger log.Logger) error {
		return GenericStageUnwindImpl(ctx, rwTx, cfg, u, executor)
	}
}

// That shouldn't be module exported, but we use this function on integration
// tool for manual unwinds.
func GenericStageUnwindImpl(ctx context.Context, tx kv.RwTx, cfg ContractAnalyzerCfg, u *UnwindState, executor UnwindExecutor) (err error) {
	useExternalTx := tx != nil

	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	// Don't display verbose start/finish logs on short range executions; given short == <= N blocks
	isShortInterval := u.CurrentBlockNumber-u.UnwindPoint <= SHORT_RANGE_EXECUTION_THRESHOLD

	if !isShortInterval {
		log.Info(fmt.Sprintf("[%s] Unwind started", u.LogPrefix()), "from", u.CurrentBlockNumber, "to", u.UnwindPoint)
	}
	if executor == nil {
		log.Warn("Unwinder executor is nil; this should only happen on test/dev code, otherwise this msg must be considered a bug")
	} else {
		if err := executor(ctx, tx, u, cfg.blockReader, isShortInterval, logEvery); err != nil {
			return err
		}
	}
	if !isShortInterval {
		log.Info(fmt.Sprintf("[%s] Unwind finished", u.LogPrefix()), "latest", u.UnwindPoint)
	}

	if err = u.Done(tx); err != nil {
		return err
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// This is a no-op implementation of stage pruning function to be used as
// a filler for stages that don't support pruning.
func NoopStagePrune(ctx context.Context, cfg ContractAnalyzerCfg) PruneFunc {
	// TODO(ots2-rebase): PruneFunc signature changed
	return func(p *PruneState, tx kv.RwTx, logger log.Logger) (err error) {
		return nil
	}
}
