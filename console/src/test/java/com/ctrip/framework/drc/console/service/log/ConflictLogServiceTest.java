package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.log.ConflictRowsLogTblDao;
import com.ctrip.framework.drc.console.dao.log.ConflictTrxLogTblDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictRowsLogTbl;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictTrxLogTbl;
import com.ctrip.framework.drc.console.dao.v2.ColumnsFilterTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.FilterTypeEnum;
import com.ctrip.framework.drc.console.param.log.ConflictRowsLogQueryParam;
import com.ctrip.framework.drc.console.param.log.ConflictTrxLogQueryParam;
import com.ctrip.framework.drc.console.service.v2.DrcBuildServiceV2;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.vo.log.ConflictCurrentRecordView;
import com.ctrip.framework.drc.console.vo.log.ConflictRowsLogView;
import com.ctrip.framework.drc.console.vo.log.ConflictTrxLogDetailView;
import com.ctrip.framework.drc.console.vo.log.ConflictTrxLogView;
import com.ctrip.framework.drc.console.vo.v2.DbReplicationView;
import com.ctrip.framework.drc.fetcher.conflict.ConflictRowLog;
import com.ctrip.framework.drc.fetcher.conflict.ConflictTransactionLog;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.console.service.v2.MigrateEntityBuilder.*;

/**
 * Created by dengquanliang
 * 2023/10/16 14:39
 */
public class ConflictLogServiceTest {

    @InjectMocks
    private ConflictLogServiceImpl conflictLogService;
    @Mock
    private ConflictTrxLogTblDao conflictTrxLogTblDao;
    @Mock
    private ConflictRowsLogTblDao conflictRowsLogTblDao;
    @Mock
    private MhaTblV2Dao mhaTblV2Dao;
    @Mock
    private DcTblDao dcTblDao;
    @Mock
    private ColumnsFilterTblV2Dao columnsFilterTblV2Dao;
    @Mock
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Mock
    private MysqlServiceV2 mysqlService;
    @Mock
    private DrcBuildServiceV2 drcBuildServiceV2;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGetConflictTrxLogView() throws Exception {
        ConflictTrxLogQueryParam param = new ConflictTrxLogQueryParam();
        Mockito.when(conflictTrxLogTblDao.queryByParam(param)).thenReturn(buildConflictTrxLogTbls());

        List<ConflictTrxLogView> result = conflictLogService.getConflictTrxLogView(param);
        Assert.assertEquals(1, result.size());
    }

    @Test
    public void testGetConflictRowsLogView() throws Exception {
        ConflictRowsLogQueryParam param = new ConflictRowsLogQueryParam();
        param.setGtid("gtid");

        Mockito.when(conflictTrxLogTblDao.queryByGtid(Mockito.anyString())).thenReturn(buildConflictTrxLogTbls().get(0));
        Mockito.when(conflictRowsLogTblDao.queryByParam(param)).thenReturn(buildConflictRowsLogTbls());
        Mockito.when(conflictTrxLogTblDao.queryByIds(Mockito.anyList())).thenReturn(buildConflictTrxLogTbls());
        Mockito.when(mhaTblV2Dao.queryByMhaNames(Mockito.anyList())).thenReturn(getMhaTbls());
        Mockito.when(dcTblDao.queryAllExist()).thenReturn(getDcTbls());

        List<ConflictRowsLogView> result = conflictLogService.getConflictRowsLogView(param);
        Assert.assertEquals(1, result.size());

    }

    @Test
    public void testGetConflictTrxLogDetailView() throws Exception {
        Mockito.when(conflictTrxLogTblDao.queryById(Mockito.anyLong())).thenReturn(buildConflictTrxLogTbls().get(0));
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.anyString())).thenReturn(getMhaTbls().get(0));
        Mockito.when(dcTblDao.queryById(Mockito.anyLong())).thenReturn(getDcTbls().get(0));
        Mockito.when(conflictRowsLogTblDao.queryByTrxLogId(Mockito.anyLong())).thenReturn(buildConflictRowsLogTbls());

        ConflictTrxLogDetailView result = conflictLogService.getConflictTrxLogDetailView(1L);
        Assert.assertEquals(1, result.getRowsLogDetailViews().size());
    }

    @Test
    public void testCreateConflict() throws Exception {
        Mockito.when(conflictTrxLogTblDao.batchInsertWithReturnId(Mockito.anyList())).thenReturn(buildConflictTrxLogTbls());
        Mockito.when(conflictRowsLogTblDao.insert(Mockito.anyList())).thenReturn(new int[1]);

        ConflictTransactionLog trxLog = buildConflictTransactionLog();
        conflictLogService.createConflictLog(Lists.newArrayList(trxLog));
    }

    @Test
    public void testGetConflictCurrentRecordView() throws Exception {
        Mockito.when(conflictTrxLogTblDao.queryById(Mockito.anyLong())).thenReturn(buildConflictTrxLogTbls().get(0));
        Mockito.when(conflictRowsLogTblDao.queryByTrxLogId(Mockito.anyLong())).thenReturn(buildConflictRowsLogTbls());
        Mockito.when( mhaTblV2Dao.queryByMhaName(Mockito.eq("srcMha"))).thenReturn(getMhaTbls().get(0));
        Mockito.when( mhaTblV2Dao.queryByMhaName(Mockito.eq("dstMha"))).thenReturn(getMhaTbls().get(1));
        Mockito.when(drcBuildServiceV2.getDbReplicationView(Mockito.anyString(), Mockito.anyString())).thenReturn(getDbReplicationViews());
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Mockito.anyList())).thenReturn(getFilterMappings());
        Mockito.when(columnsFilterTblV2Dao.queryByIds(Mockito.anyList())).thenReturn(Lists.newArrayList(getColumnsFilterTbl()));
//        Mockito.when(mysqlService.queryTableRecords(Mockito.eq("srcMha"), Mockito.anyString())).thenReturn(getSrcResMap());
//        Mockito.when(mysqlService.queryTableRecords(Mockito.eq("dstMha"), Mockito.anyString())).thenReturn(getDstResMap());

        ConflictCurrentRecordView result = conflictLogService.getConflictCurrentRecordView(1L);
        Assert.assertTrue(result.isRecordIsEqual());
    }

    @Test
    public void testDeleteTrxLogs() throws Exception {
        Mockito.when(conflictTrxLogTblDao.queryByHandleTime(Mockito.anyLong(), Mockito.anyLong())).thenReturn(buildConflictTrxLogTbls());
        Mockito.when(conflictRowsLogTblDao.queryByTrxLogIds(Mockito.anyList())).thenReturn(buildConflictRowsLogTbls());

        long result = conflictLogService.deleteTrxLogs(0, System.currentTimeMillis());
        Assert.assertEquals(result, buildConflictRowsLogTbls().size());
    }

    private Map<String, Object> getSrcResMap() {
        Map<String, Object> res = new HashMap<>();
        res.put("tableName", "db.table");

        Map<String, Object> records = new HashMap<>();
        records.put("id", 1L);
        records.put("column", "a");
        res.put("record", Lists.newArrayList(records));

        Map<String, Object> metaColumn = new HashMap<>();
        metaColumn.put("key", "column");
        res.put("metaColumn", Lists.newArrayList(metaColumn));

        res.put("columns", Lists.newArrayList("column"));
        return res;
    }

    private Map<String, Object> getDstResMap() {
        Map<String, Object> res = new HashMap<>();
        res.put("tableName", "tableName");

        Map<String, Object> records = new HashMap<>();
        records.put("id", 1L);
        records.put("column", "b");
        res.put("record", Lists.newArrayList(records));

        Map<String, Object> metaColumn = new HashMap<>();
        metaColumn.put("key", "column");
        res.put("metaColumn", Lists.newArrayList(metaColumn));

        res.put("columns", Lists.newArrayList("column"));
        return res;
    }

    private List<DbReplicationView> getDbReplicationViews() {
        DbReplicationView view = new DbReplicationView();
        view.setDbReplicationId(200L);
        view.setDbName("db");
        view.setLogicTableName("table");
        view.setFilterTypes(Lists.newArrayList(FilterTypeEnum.COLUMNS_FILTER.getCode()));
        return Lists.newArrayList(view);
    }

    private ConflictTransactionLog buildConflictTransactionLog() {
        ConflictTransactionLog trxLog = new ConflictTransactionLog();
        trxLog.setSrcMha("srcMha");
        trxLog.setDstMha("dstMha");
        trxLog.setGtid("gtid");
        trxLog.setTrxRowsNum(1L);
        trxLog.setCflRowsNum(2L);
        trxLog.setHandleTime(System.currentTimeMillis());
        trxLog.setTrxRes(0);
        trxLog.setCflLogs(Lists.newArrayList(buildConflictRowLog()));
        return trxLog;
    }

    private ConflictRowLog buildConflictRowLog() {
        ConflictRowLog rowLog = new ConflictRowLog();
        rowLog.setDb("db");
        rowLog.setTable("table");
        rowLog.setRawSql("rawSql");
        rowLog.setRawRes("rawRes");
        rowLog.setDstRecord("dstRecord");
        rowLog.setHandleSql("handleSql");
        rowLog.setHandleSqlRes("handleSqlRes");
        return rowLog;
    }

    private List<ConflictTrxLogTbl> buildConflictTrxLogTbls() {
        ConflictTrxLogTbl tbl = new ConflictTrxLogTbl();
        tbl.setId(1L);
        tbl.setTrxResult(0);
        tbl.setSrcMhaName("srcMha");
        tbl.setDstMhaName("dstMha");
        tbl.setGtid("gtid");
        tbl.setTrxRowsNum(1L);
        tbl.setCflRowsNum(1L);
        tbl.setHandleTime(System.currentTimeMillis());
        return Lists.newArrayList(tbl);
    }

    private List<ConflictRowsLogTbl> buildConflictRowsLogTbls() {
        ConflictRowsLogTbl tbl = new ConflictRowsLogTbl();
        tbl.setId(1L);
        tbl.setConflictTrxLogId(1L);
        tbl.setDbName("dbName");
        tbl.setTableName("tableName");
        tbl.setRawSql("rawSql");
        tbl.setRawSqlResult("rawSqlResult");
        tbl.setDstRowRecord("dstRowRecord");
        tbl.setHandleSql("handleSql");
        tbl.setHandleSqlResult("handleSqlResult");
        tbl.setRowResult(0);
        tbl.setHandleTime(System.currentTimeMillis());
        tbl.setRowId(1L);

        return Lists.newArrayList(tbl);
    }

    private List<MhaTblV2> getMhaTbls() {
        MhaTblV2 mha0 = new MhaTblV2();
        mha0.setMhaName("srcMha");
        mha0.setDcId(200L);

        MhaTblV2 mha1 = new MhaTblV2();
        mha1.setMhaName("dstMha");
        mha1.setDcId(201L);
        return Lists.newArrayList(mha0, mha1);
    }
}
