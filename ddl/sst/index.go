package sst

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"

	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/util/sqlexec"

	"github.com/twmb/murmur3"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	tidbcfg "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util/logutil"
)

func InitIndexOptimize() {
	cfg := tidbcfg.GetGlobalConfig()
	cluster.PdAddr = cfg.AdvertiseAddress
	cluster.Port = cfg.Port
	cluster.Status = cfg.Status.StatusPort
	cluster.PdAddr = cfg.Path
	fmt.Printf("InitOnce %+v.ck=%v\n", cluster, *ck)
	log.SetAppLogger(logutil.BgLogger())
}

// TODO: 1. checkpoint??
// TODO: 2. EngineID can use startTs for only.
func PrepareIndexOp(ctx context.Context, ddl DDLInfo) error {
	_, err := ec.getEngineInfo(ddl.StartTs)
	if err == nil {
		// has found it ;
		LogDebug("ddl %d has exist.", ddl.StartTs)
		ec.releaseRef(ddl.StartTs)
		return nil
	}
	if err != ErrNotFound {
		return err
	}
	LogInfo("PrepareIndexOp %+v", ddl)
	// err == ErrNotFound
	info := cluster
	be, err := createLocalBackend(ctx, info)
	if err != nil {
		LogFatal("PrepareIndexOp.createLocalBackend err:%s.", err.Error())
		return fmt.Errorf("PrepareIndexOp.createLocalBackend err:%w", err)
	}
	cpt := checkpoints.TidbTableInfo{
		genNextTblId(),
		ddl.Schema,
		ddl.Table.Name.String(),
		ddl.Table,
	}
	var cfg backend.EngineConfig
	cfg.TableInfo = &cpt
	//
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], ddl.StartTs)
	h := murmur3.New32()
	h.Write(b[:])
	en, err := be.OpenEngine(ctx, &cfg, ddl.Table.Name.String(), int32(h.Sum32()))
	if err != nil {
		return fmt.Errorf("PrepareIndexOp.OpenEngine err:%w", err)
	}
	ec.put(ddl.StartTs, &cfg, en, ddl.Table)
	return nil
}

func IndexOperator(ctx context.Context, startTs uint64, k, v []byte) error {
	LogFatal("IndexOperator logic error")
	LogDebug("IndexOperator '%x','%x'", k, v)
	ei, err := ec.getEngineInfo(startTs)
	if err != nil {
		return err
	}
	defer ec.releaseRef(startTs)
	//

	ei.pushKV(k, v)
	kvSize := ei.size
	if kvSize < flush_size {
		return nil
	}
	//
	return flushKvs(ctx, ei)
}

func flushKvs(ctx context.Context, ei *engineInfo) error {
	if len(ei.kvs) <= 0 {
		return nil
	}
	if ctx == nil {
		// this may be nil,and used in WriteRows;
		ctx = context.TODO()
	}
	LogInfo("flushKvs (%d)", len(ei.kvs))
	lw, err := ei.getWriter()
	if err != nil {
		return fmt.Errorf("IndexOperator.getWriter err:%w", err)
	}

	err = lw.WriteRows(ctx, nil, kv.NewKvPairs(ei.kvs))
	if err != nil {
		return fmt.Errorf("IndexOperator.WriteRows err:%w", err)
	}
	ei.ResetCache()
	return nil
}

func FlushKeyValSync(ctx context.Context, startTs uint64, cache *WorkerKVCache) error {
	ec.mtx.RLock()
	ei, ok := ec.cache[startTs]
	defer ec.mtx.RUnlock()
	if !ok {
		return ErrNotFound
	}
	lw, err := ei.getWriter()
	if err != nil {
		return fmt.Errorf("IndexOperator.getWriter err:%w", err)
	}
	err = lw.WriteRows(ctx, nil, kv.NewKvPairs(cache.Fetch()))
	if err != nil {
		return fmt.Errorf("IndexOperator.WriteRows err:%w", err)
	}
	ei.size += cache.Size()
	return nil
}

func fetchTableRegionSizeStats(tblId int64, exec sqlexec.RestrictedSQLExecutor) (ret map[uint64]int64, err error) {
	// must use '%?' to replace '?' in RestrictedSQLExecutor.
	query := "SELECT REGION_ID, APPROXIMATE_SIZE FROM information_schema.TIKV_REGION_STATUS WHERE TABLE_ID = %?"
	sn, err := exec.ParseWithParams(context.TODO(), query, tblId)
	if err != nil {
		return nil, fmt.Errorf("ParseWithParams err:%w", err)
	}
	rows, _, err := exec.ExecRestrictedStmt(context.TODO(), sn)
	if err != nil {
		return nil, fmt.Errorf("ExecRestrictedStmt err:%w", err)
	}
	// parse values;
	ret = make(map[uint64]int64, len(rows))
	var (
		regionID uint64
		size     int64
	)
	for idx, row := range rows {
		if 2 != row.Len() {
			return nil, fmt.Errorf("row %d has %d fields", idx, row.Len())
		}
		regionID = row.GetUint64(0)
		size = row.GetInt64(1)
		ret[regionID] = size
	}
	//
	d, _ := json.Marshal(ret)
	LogTest("fetchTableRegionSizeStats table(%d) = %s.", tblId, string(d))
	return ret, nil
}

func FinishIndexOp(ctx context.Context, startTs uint64, exec sqlexec.RestrictedSQLExecutor) error {
	ei, err := ec.getEngineInfo(startTs)
	if err != nil {
		return err
	}
	flushKvs(ctx, ei)
	defer ec.releaseRef(startTs)
	//
	LogInfo("FinishIndexOp %d;kvs=%d.", startTs, ei.size)
	//
	ret, err := fetchTableRegionSizeStats(ei.tbl.ID, exec)
	if err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.TODO()
	}
	ctx = context.WithValue(ctx, local.RegionSizeStats, ret)
	//
	indexEngine := ei.OpenedEngine
	cfg := ei.cfg
	//
	closeEngine, err1 := indexEngine.Close(ctx, cfg)
	if err1 != nil {
		return fmt.Errorf("engine.Close err:%w", err1)
	}
	pdController, err := pdutil.NewPdController(ctx, cluster.PdAddr,
		nil, pd.SecurityOption{})
	if err != nil {
		return errors.Errorf("fail to create pd controller: %v", err)
	}
	orig, _, err := pdController.RemoveSchedulersWithOrigin(ctx)
	if err != nil {
		return errors.Errorf("fail to create pd controller: %v", err)
	}
	cancelFunc := pdController.MakeUndoFunctionByConfig(orig)
	defer func() {
		err := cancelFunc(ctx)
		if err != nil {
			LogError("fail to undo remove schedule operations", zap.Error(err))
		}
	}()
	// use default value first;
	err = closeEngine.Import(ctx, int64(config.SplitRegionSize))
	if err != nil {
		return fmt.Errorf("engine.Import err:%w", err)
	}
	err = closeEngine.Cleanup(ctx)
	if err != nil {
		return fmt.Errorf("engine.Cleanup err:%w", err)
	}
	// should release before ReleaseEngine
	ec.releaseRef(startTs)
	ec.ReleaseEngine(startTs)
	return nil
}
