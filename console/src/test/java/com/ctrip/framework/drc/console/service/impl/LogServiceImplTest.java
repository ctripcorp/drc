package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dto.ConflictTransactionLog;
import com.ctrip.framework.drc.console.dto.LogDto;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.DalQueryDao;
import com.ctrip.platform.dal.dao.StatementParameters;
import com.ctrip.platform.dal.dao.sqlbuilder.FreeUpdateSqlBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.*;

/**
 * Created by jixinwang on 2020/6/22
 */
public class LogServiceImplTest {


    @Mock
    private DalQueryDao queryDao;

    @Mock
    private ApplierUploadLogTblDao dao;

    @Mock
    private ConflictLogDao conflictLogDao;

    @Mock
    private MhaTblDao mhaTblDao;

    @Mock
    private DcTblDao dcTblDao;

    @Mock
    private MhaGroupTblDao mhaGroupTblDao;

    @Mock
    private MachineTblDao machineTblDao;
    
    @Mock
    private MetaInfoServiceImpl metaInfoService;
    
    @Mock
    private DefaultConsoleConfig defaultConsoleConfig;

    @InjectMocks
    private LogServiceImpl logServiceImpl;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        Mockito.when(defaultConsoleConfig.getConflictMhaRecordSearchTime()).thenReturn(120);
    }

    @Test
    public void testUploadConflictLog() {
        List<ConflictTransactionLog> conflictTransactionLogList = new ArrayList<ConflictTransactionLog>();
        ConflictTransactionLog conflictTransactionLog = new ConflictTransactionLog();
        conflictTransactionLog.setSrcMhaName("src");
        conflictTransactionLog.setDestMhaName("dest");
        conflictTransactionLog.setClusterName("testCluster");
        List<String> sqlList = new ArrayList<String>();
        sqlList.add("sql1");
        sqlList.add("sql2");
        conflictTransactionLog.setRawSqlList(sqlList);
        conflictTransactionLog.setRawSqlExecutedResultList(sqlList);
        conflictTransactionLog.setDestCurrentRecordList(sqlList);
        conflictTransactionLog.setConflictHandleSqlList(sqlList);
        conflictTransactionLog.setConflictHandleSqlExecutedResultList(sqlList);
        conflictTransactionLog.setConflictHandleTime(System.currentTimeMillis());
        conflictTransactionLog.setLastResult("lastResult");
        conflictTransactionLogList.add(conflictTransactionLog);
        logServiceImpl.uploadConflictLog(conflictTransactionLogList);
    }

    @Test
    public void testUploadSampleLog() throws SQLException {
        LogDto logDto = new LogDto();
        logDto.setSrcDcName("src");
        logDto.setDestDcName("dest");
        logDto.setUserId("user");
        logDto.setSqlStatement("sqlStatement");
        Mockito.when(dao.insert(Mockito.any(DalHints.class), Mockito.any(ApplierUploadLogTbl.class))).thenReturn(1);
        logServiceImpl.uploadSampleLog(logDto);
    }

    @Test
    public void testDeleteLog() throws SQLException {
        Mockito.when(queryDao.update(Mockito.any(FreeUpdateSqlBuilder.class), Mockito.any(StatementParameters.class), Mockito.any(DalHints.class))).thenReturn(1);
        logServiceImpl.deleteLog(new DalHints());
    }

    @Test
    public void testGetLogs() throws SQLException {
        List<ApplierUploadLogTbl> list = new ArrayList<>();
        Mockito.when(dao.count()).thenReturn(10);
        Mockito.when(dao.queryLogByPage(Mockito.anyInt(), Mockito.anyInt(), Mockito.any(DalHints.class))).thenReturn(list);
        Map map = logServiceImpl.getLogs(1, 10);
    }

    @Test
    public void testGetLogsSearch() throws SQLException {
        List<ApplierUploadLogTbl> list = new ArrayList<>();
        Mockito.when(dao.count(Mockito.anyString())).thenReturn(5);
        Mockito.when(dao.queryLogByPage(Mockito.anyInt(), Mockito.anyInt(), Mockito.anyString(), Mockito.any(DalHints.class))).thenReturn(list);
        Map map = logServiceImpl.getLogs(1, 10, "test");
    }

    @Test
    public void testParseSql() {
        String updateSql = "/*DRC UPDATE 2*/ UPDATE `drc4`.`insert3` SET `one`='a4' where `id`=1";
//        String deleteSql = "/*DRC DELETE 0*/ DELETE FROM `commonordershard6db`.`basicorder` WHERE `ID`=8300013702739991 AND `DataChange_LastTime`='2020-10-27 00:54:47.723'";
        Map<String, String> parseResult = logServiceImpl.parseSql(updateSql);
//        Assert.assertEquals("Delete", parseResult.get("manipulation"));
        Assert.assertEquals("Update", parseResult.get("manipulation"));
    }

    @Test
    public void testGetConflictLog() throws SQLException {
        Mockito.when(conflictLogDao.count(Mockito.anyString())).thenReturn(5);
        Map<String, Object> map = logServiceImpl.getConflictLog(1, 10, "test");
        Assert.assertEquals(5, map.get("count"));
    }

    @Test
    public void testGetCurrentRecord() throws Exception {
        ConflictLog conflictLog = new ConflictLog();
        conflictLog.setRawSqlList("rawSql1\n rawSql2\n rawSql3");
        conflictLog.setSrcMhaName("srcMhaName");
        conflictLog.setDestMhaName("destMhaName");
        conflictLog.setRawSqlExecutedResultList("rawSqlExecutedResult");
        conflictLog.setDestCurrentRecordList("destCurrentRecord");
        conflictLog.setConflictHandleSqlList("conflictHandleSql");
        conflictLog.setConflictHandleSqlExecutedResultList("conflictHandleSqlExecutedResult");
        conflictLog.setLastResult("lastResult");
        Map srcMap = new HashMap();
        srcMap.put("column", "column");
        srcMap.put("record", "record");
        LogServiceImpl serviceImpl = Mockito.spy(logServiceImpl);
        Mockito.doReturn(srcMap).when(serviceImpl).selectRecord(Mockito.anyString(), Mockito.anyString(),Mockito.anyString());
        Mockito.doReturn(true).when(serviceImpl).markTableDataDiff(Mockito.anyMap(), Mockito.anyMap());
        Mockito.doReturn("dcName").when(serviceImpl).getDcNameByMhaName(Mockito.anyString());
        Mockito.when(conflictLogDao.queryByPk(Mockito.anyLong())).thenReturn(conflictLog);
        Map ret = serviceImpl.getCurrentRecord(1L);
        Assert.assertEquals(true, ret.get("diffTransaction"));
    }

    @Test
    public void testGetDcNameByMhaName() throws SQLException {
        List<MhaTbl> mhaList = new ArrayList<>();
        MhaTbl mhaTbl = new MhaTbl();
        mhaTbl.setDcId(1L);
        mhaList.add(mhaTbl);
        Mockito.when(mhaTblDao.queryBy(Mockito.any(MhaTbl.class))).thenReturn(mhaList);
        List<DcTbl> dcList = new ArrayList<>();
        DcTbl dcTbl = new DcTbl();
        dcTbl.setDcName("testDcName");
        dcList.add(dcTbl);
        Mockito.when(dcTblDao.queryBy(Mockito.any(DcTbl.class))).thenReturn(dcList);
        String dcName = logServiceImpl.getDcNameByMhaName("testMhaName");
        Assert.assertEquals("testDcName", dcName);
    }

    @Test
    public void testInitSqlOperator() throws Exception {
        List<MhaTbl> mhaList = new ArrayList<>();
        MhaTbl mhaTbl = new MhaTbl();
        mhaTbl.setId(1L);
        mhaTbl.setMhaGroupId(2L);
        mhaList.add(mhaTbl);
        Mockito.when(mhaTblDao.queryBy(Mockito.any(MhaTbl.class))).thenReturn(mhaList);
        
        MhaGroupTbl mhaGroupTbl = new MhaGroupTbl();
        mhaGroupTbl.setMonitorUser("testUser");
        mhaGroupTbl.setMonitorPassword("testPassword");
        Mockito.when(mhaGroupTblDao.queryByPk(Mockito.anyLong())).thenReturn(mhaGroupTbl);

        List<MachineTbl> machineTblList = new ArrayList<>();
        MachineTbl machineTbl = new MachineTbl();
        machineTbl.setIp("120.0.0.1");
        machineTbl.setPort(8080);
        machineTblList.add(machineTbl);
        Mockito.when(machineTblDao.queryBy(Mockito.any(MachineTbl.class))).thenReturn(machineTblList);

        WriteSqlOperatorWrapper writeSqlOperatorWrapper = 
                logServiceImpl.initSqlOperator("testMhaName","testMhaName");
        Assert.assertNotNull(writeSqlOperatorWrapper);
    }

    @Test
    public void testResultSetConvertList() {

    }

    @Test
    public void markTableDataDiff() {
        Map srcMap = new HashMap();
        Map destMap = new HashMap();
        List<String> srcColumnList = Arrays.asList("col1", "col2", "col3");
        List<String> destColumnList = Arrays.asList("col1", "col2", "col4");
        srcMap.put("columnList", srcColumnList);
        destMap.put("columnList", destColumnList);
        Map<String, Object> srcRecord = new HashMap<>();
        Map<String, Object> destRecord = new HashMap<>();
        srcRecord.put("col1", "val1");
        srcRecord.put("col2", "val2-1");
        srcRecord.put("col3", "val3");
        destRecord.put("col1", "val1");
        destRecord.put("col2", "val2-2");
        destRecord.put("col4", "val4");
        List<Map<String, Object>> srcRecordList = new ArrayList<>();
        List<Map<String, Object>> destRecordList = new ArrayList<>();
        srcRecordList.add(srcRecord);
        destRecordList.add(destRecord);
        srcMap.put("record", srcRecordList);
        destMap.put("record", destRecordList);
        LogServiceImpl serviceImpl = Mockito.spy(logServiceImpl);
        Mockito.doReturn(true).when(serviceImpl).markExtraColumn(Mockito.any(Map.class), Mockito.any(List.class));
        Mockito.doReturn(true).when(serviceImpl).markDiffRecord(Mockito.any(Map.class), Mockito.any(Map.class), Mockito.any(List.class));
        boolean tableDataDiff = serviceImpl.markTableDataDiff(srcMap, destMap);
        Assert.assertEquals(true, tableDataDiff);
    }

    @Test
    public void markExtraColumn() {
        Map<String, Object> record = new HashMap<>();
        Map<String, String> cellClassName = new HashMap();
        record.put("cellClassName", cellClassName);
        record.put("column1", "value1");
        List<String> columnDiff = new ArrayList<>();
        columnDiff.add("column1");
        boolean hasExtraColumn = logServiceImpl.markExtraColumn(record, columnDiff);
        Assert.assertEquals(true, hasExtraColumn);
    }

    @Test
    public void markDiffRecord() {
        Map<String, Object> srcRecord = new HashMap<>();
        Map<String, Object> destRecord = new HashMap<>();
        Map<String, String> srcCellClassName = new HashMap();
        Map<String, String> destCellClassName = new HashMap();
        srcRecord.put("cellClassName", srcCellClassName);
        destRecord.put("cellClassName", destCellClassName);
        srcRecord.put("col1", null);
        srcRecord.put("col2", null);
        srcRecord.put("col3", "value3-1");
        srcRecord.put("col4", "value4");
        destRecord.put("col1", null);
        destRecord.put("col2", "value2");
        destRecord.put("col3", "value3-2");
        destRecord.put("col4", "value4");
        List<String> intersectionColumnList = Arrays.asList("col1", "col2", "col3", "col4");
        boolean hasDiffRecord = logServiceImpl.markDiffRecord(srcRecord, destRecord, intersectionColumnList);
        Assert.assertEquals(true, hasDiffRecord);
    }
}
