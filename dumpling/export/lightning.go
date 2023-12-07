package export

import (
	"context"
	"net"
	"runtime"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/util"
	"go.uber.org/multierr"
)

const (
	statusPort      = "10080"
	pdPath          = "127.0.0.1:2379"
	sortDir         = "/tmp/tidb_lightning_sort_dir"
	fullTableName   = "`test`.`clustered_cache1`"
	regionSplitKeys = config.SplitRegionKeys
	regionSplitSize = config.SplitRegionSize
)

func newLocalBackend(ctx context.Context, conf *Config) (*local.Backend, error) {
	hostPort := net.JoinHostPort("127.0.0.1", statusPort)
	tls, err := common.NewTLS(
		conf.Security.CAPath,
		conf.Security.CertPath,
		conf.Security.KeyPath,
		hostPort,
		nil, nil, nil,
	)
	if err != nil {
		return nil, err
	}

	path := pdPath
	dir := sortDir
	// no need to close kvStore, since it's a cached store.
	// kvStore, err := GetCachedKVStoreFrom(path, tls)
	// if err != nil {
	// 	return nil, errors.Trace(err)
	// }

	backendConfig := local.BackendConfig{
		PDAddr:                 path,
		LocalStoreDir:          dir,
		MaxConnPerStore:        config.DefaultRangeConcurrency,
		ConnCompressType:       config.CompressionNone,
		WorkerConcurrency:      config.DefaultRangeConcurrency * 2,
		KVWriteBatchSize:       config.KVWriteBatchSize,
		RegionSplitBatchSize:   config.DefaultRegionSplitBatchSize,
		RegionSplitConcurrency: runtime.GOMAXPROCS(0),
		// enable after we support checkpoint
		CheckpointEnabled:       false,
		MemTableSize:            config.DefaultEngineMemCacheSize,
		LocalWriterMemCacheSize: int64(config.DefaultLocalWriterMemCacheSize),
		ShouldCheckTiKV:         true,
		DupeDetectEnabled:       false,
		DuplicateDetectOpt:      common.DupDetectOpt{ReportErrOnDup: false},
		MaxOpenFiles:            int(util.GenRLimit("table_import")),
		PausePDSchedulerScope:   config.PausePDSchedulerScopeTable,
	}

	// todo: use a real region size getter
	regionSizeGetter := &local.TableRegionSizeGetterImpl{}
	localBackend, err := local.NewBackend(ctx, tls, backendConfig, regionSizeGetter)
	if err != nil {
		return nil, err
	}
	return localBackend, nil
}

func openDataEngine(ctx context.Context, tableInfo *checkpoints.TidbTableInfo, localBackend *local.Backend, fullTableName string, engineID int32) (*backend.OpenedEngine, error) {
	dataEngineCfg := &backend.EngineConfig{
		TableInfo: tableInfo,
	}
	// todo: support checking IsRowOrdered later.
	//if ti.tableMeta.IsRowOrdered {
	//	dataEngineCfg.Local.Compact = true
	//	dataEngineCfg.Local.CompactConcurrency = 4
	//	dataEngineCfg.Local.CompactThreshold = local.CompactionUpperThreshold
	//}
	mgr := backend.MakeEngineManager(localBackend)
	return mgr.OpenEngine(ctx, dataEngineCfg, fullTableName, engineID)
}

func openIndexEngine(ctx context.Context, tableInfo *checkpoints.TidbTableInfo, localBackend *local.Backend, fullTableName string, engineID int32) (*backend.OpenedEngine, error) {
	idxEngineCfg := &backend.EngineConfig{
		TableInfo: tableInfo,
	}
	idxCnt := len(tableInfo.Core.Indices)
	if !common.TableHasAutoRowID(tableInfo.Core) {
		idxCnt--
	}
	// todo: getTotalRawFileSize returns size of all data files, but in distributed framework,
	// we create one index engine for each engine, should reflect this in the future.
	// threshold := local.EstimateCompactionThreshold2(ti.getTotalRawFileSize(int64(idxCnt)))
	// idxEngineCfg.Local = backend.LocalEngineConfig{
	// 	Compact:            threshold > 0,
	// 	CompactConcurrency: 4,
	// 	CompactThreshold:   threshold,
	// }
	// fullTableName := ti.fullTableName()
	// todo: cleanup all engine data on any error since we don't support checkpoint for now
	// some return path, didn't make sure all data engine and index engine are cleaned up.
	// maybe we can add this in upper level to clean the whole local-sort directory
	mgr := backend.MakeEngineManager(localBackend)
	return mgr.OpenEngine(ctx, idxEngineCfg, fullTableName, engineID)
}

func importAndCleanup(ctx context.Context, closedEngine *backend.ClosedEngine, backend *local.Backend) (int64, error) {
	var kvCount int64
	importErr := closedEngine.Import(ctx, int64(regionSplitSize), int64(regionSplitKeys))
	if closedEngine.GetID() != common.IndexEngineID {
		// todo: change to a finer-grain progress later.
		// each row is encoded into 1 data key
		kvCount = backend.GetImportedKVCount(closedEngine.GetUUID())
	}
	// todo: if we need support checkpoint, engine should not be cleanup if import failed.
	cleanupErr := closedEngine.Cleanup(ctx)
	return kvCount, multierr.Combine(importErr, cleanupErr)
}
