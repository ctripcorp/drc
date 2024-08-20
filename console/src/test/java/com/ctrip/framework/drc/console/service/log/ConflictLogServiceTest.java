package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.log.ConflictDbBlackListTblDao;
import com.ctrip.framework.drc.console.dao.log.ConflictRowsLogTblDao;
import com.ctrip.framework.drc.console.dao.log.ConflictTrxLogTblDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictDbBlackListTbl;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictRowsLogCount;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictRowsLogTbl;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictTrxLogTbl;
import com.ctrip.framework.drc.console.dao.v2.ColumnsFilterTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.ReplicationTableTblDao;
import com.ctrip.framework.drc.console.enums.FilterTypeEnum;
import com.ctrip.framework.drc.console.enums.log.CflBlacklistType;
import com.ctrip.framework.drc.console.param.log.ConflictAutoHandleParam;
import com.ctrip.framework.drc.console.param.log.ConflictDbBlacklistQueryParam;
import com.ctrip.framework.drc.console.param.log.ConflictRowsLogQueryParam;
import com.ctrip.framework.drc.console.param.log.ConflictTrxLogQueryParam;
import com.ctrip.framework.drc.console.param.mysql.QueryRecordsRequest;
import com.ctrip.framework.drc.console.service.v2.DrcBuildServiceV2;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.utils.CommonUtils;
import com.ctrip.framework.drc.console.utils.Constants;
import com.ctrip.framework.drc.console.vo.log.*;
import com.ctrip.framework.drc.console.vo.v2.DbReplicationView;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.service.user.IAMService;
import com.ctrip.framework.drc.fetcher.conflict.ConflictRowLog;
import com.ctrip.framework.drc.fetcher.conflict.ConflictTransactionLog;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.util.StopWatch;

import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.*;
import static org.mockito.Mockito.*;
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
    @Mock
    private ConflictDbBlackListTblDao conflictDbBlackListTblDao;
    @Mock
    private DbaApiService dbaApiService;
    @Mock
    private IAMService iamService;
    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private DbBlacklistCache dbBlacklistCache;
    @Mock
    private DomainConfig domainConfig;
    @Mock
    private ReplicationTableTblDao replicationTableTblDao;

    @Before
    public void setUp() {
        System.setProperty("iam.config.enable", "off"); // skip the constructor of IAMServiceImpl
        MockitoAnnotations.openMocks(this);
        Mockito.when(consoleConfig.getConflictLogQueryTimeInterval()).thenReturn(Constants.ONE_DAY);
    }
    
//    @Test
//    public void testCflBlacklist() throws IOException {
//
//        List<String> dbFilters = Lists.newArrayList();
//        try (BufferedReader reader = new BufferedReader(new FileReader("src/test/resources/blacklist.txt"))) {
//            String line;
//            while ((line = reader.readLine()) != null) {
//                dbFilters.add(line);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        String dbFilter = Joiner.on(",").join(dbFilters);
//        AviatorRegexFilter regexFilter = new AviatorRegexFilter(dbFilter);
//        List<AviatorRegexFilter> regexFilterList = dbFilters.stream().map(AviatorRegexFilter::new).collect(Collectors.toList());
//
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start("task1");
//        
////        regexFilter.filter("fltfullskilldb"+"."+ "asproductorder");
//        
//        stopWatch.stop();
//        System.out.println(stopWatch.getLastTaskTimeMillis());
//
//        stopWatch.start("task2");
//        for (int i = 0; i < 10; i++) {
//            for (AviatorRegexFilter filter : regexFilterList) {
//                if (filter.filter("fltfullskilldb"+"."+ "asproductorder")) {
//                    System.out.println("match");
//                    break;
//                }
//            }
//        }
//        System.out.println("no match");
//        stopWatch.stop();
//        System.out.println(stopWatch.getLastTaskTimeMillis());
//    }

    @Test
    public void testAddDbBlacklist() throws Exception {
        when(conflictDbBlackListTblDao.queryBy(anyString(),anyInt())).thenReturn(null);
        when(conflictDbBlackListTblDao.insert(any(ConflictDbBlackListTbl.class))).thenReturn(1);
        when(domainConfig.getBlacklistExpirationHour(Mockito.any())).thenReturn(1);
        doNothing().when(dbBlacklistCache).refresh(true);
        
        conflictLogService.addDbBlacklist("db1\\.table1", CflBlacklistType.NO_USER_TRAFFIC,System.currentTimeMillis() + Constants.ONE_DAY);
        conflictLogService.addDbBlacklist("db1\\.table1", CflBlacklistType.DBA_JOB,null);

        ConflictDbBlackListTbl db2table2 = new ConflictDbBlackListTbl();
        db2table2.setId(1L);
        when(conflictDbBlackListTblDao.queryBy(eq("db2\\.table2"),anyInt())).thenReturn(Lists.newArrayList(db2table2));
        conflictLogService.addDbBlacklist("db2\\.table2", CflBlacklistType.DBA_JOB,null);
        verify(conflictDbBlackListTblDao,times(2)).insert(any(ConflictDbBlackListTbl.class));
        verify(dbBlacklistCache,times(3)).refresh(true);
        verify(conflictDbBlackListTblDao,times(1)).update(any(ConflictDbBlackListTbl.class));
    }
    
    
    @Test
    public void testGetConflictTrxLogView() throws Exception {
        ConflictTrxLogQueryParam param = new ConflictTrxLogQueryParam();
        param.setBeginHandleTime(1L);
        param.setEndHandleTime(Constants.TWO_HOUR);
        Mockito.when(conflictTrxLogTblDao.queryByParam(param)).thenReturn(buildConflictTrxLogTbls());

        // case 1: can not query all db , dbsCanQuery is empty
        Mockito.when(iamService.canQueryAllDbReplication()).thenReturn(Pair.of(false, null));
        Mockito.when(dbaApiService.getDBsWithQueryPermission()).thenReturn(null);
        List<ConflictTrxLogView> result = null;
        try {
            result = conflictLogService.getConflictTrxLogView(param);
        } catch (Exception e) {
            Assert.assertEquals("no db with DOT permission!", e.getMessage());
        }

        // case 2: can not query all db , query a db with dot permission;
        Mockito.when(dbaApiService.getDBsWithQueryPermission()).thenReturn(Lists.newArrayList("db1"));
        param.setDb("db1");
        result = conflictLogService.getConflictTrxLogView(param);
        Assert.assertEquals(1, result.size());

        // case 3: can query all db
        Mockito.when(iamService.canQueryAllDbReplication()).thenReturn(Pair.of(true, null));
        result = conflictLogService.getConflictTrxLogView(param);
        Assert.assertEquals(1, result.size());

    }

    @Test
    public void testGetTrxLogCount() throws Exception {
        ConflictTrxLogQueryParam param = new ConflictTrxLogQueryParam();
        param.setBeginHandleTime(1L);
        param.setEndHandleTime(Constants.TWO_HOUR);

        Mockito.when(conflictTrxLogTblDao.getCount(Mockito.any())).thenReturn(1);

        // case 1: can not query all db , dbsCanQuery is empty
        Mockito.when(iamService.canQueryAllDbReplication()).thenReturn(Pair.of(false, null));
        Mockito.when(dbaApiService.getDBsWithQueryPermission()).thenReturn(Lists.newArrayList("db"));

        int result = conflictLogService.getTrxLogCount(param);
        Assert.assertEquals(result, 1);
    }

    @Test
    public void testGetRowsLogCount() throws Exception {
        ConflictRowsLogQueryParam param = new ConflictRowsLogQueryParam();
        param.setGtid("gtid");
        param.setBeginHandleTime(1L);
        param.setEndHandleTime(Constants.TWO_HOUR);

        Mockito.when(conflictRowsLogTblDao.getCount(Mockito.any())).thenReturn(1);
        Mockito.when(conflictTrxLogTblDao.queryByGtid(Mockito.anyString())).thenReturn(buildConflictTrxLogTbls().get(0));
        Mockito.when(conflictRowsLogTblDao.queryByParam(param)).thenReturn(buildConflictRowsLogTbls());
        Mockito.when(conflictTrxLogTblDao.queryByIds(Mockito.anyList())).thenReturn(buildConflictTrxLogTbls());
        Mockito.when(mhaTblV2Dao.queryByMhaNames(Mockito.anyList())).thenReturn(getMhaTbls());
        Mockito.when(dcTblDao.queryAllExist()).thenReturn(getDcTbls());
        Mockito.when(iamService.canQueryAllDbReplication()).thenReturn(Pair.of(true, null));

        int result = conflictLogService.getRowsLogCount(param);
        Assert.assertEquals(result, 1);
    }

    @Test
    public void testGetConflictRowsLogView() throws Exception {
        ConflictRowsLogQueryParam param = new ConflictRowsLogQueryParam();
        param.setGtid("gtid");
        param.setBeginHandleTime(1L);
        param.setEndHandleTime(2L);

        Mockito.when(conflictTrxLogTblDao.queryByGtid(Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong())).thenReturn(buildConflictTrxLogTbls().get(0));

        Mockito.when(conflictTrxLogTblDao.queryByGtid(Mockito.anyString())).thenReturn(buildConflictTrxLogTbls().get(0));
        Mockito.when(conflictRowsLogTblDao.queryByParam(param)).thenReturn(buildConflictRowsLogTbls());
        Mockito.when(conflictTrxLogTblDao.queryByIds(Mockito.anyList())).thenReturn(buildConflictTrxLogTbls());
        Mockito.when(mhaTblV2Dao.queryByMhaNames(Mockito.anyList())).thenReturn(getMhaTbls());
        Mockito.when(dcTblDao.queryAllExist()).thenReturn(getDcTbls());
        Mockito.when(iamService.canQueryAllDbReplication()).thenReturn(Pair.of(true, null));

        List<ConflictRowsLogView> result = conflictLogService.getConflictRowsLogView(param);
        Assert.assertEquals(1, result.size());
    }


    @Test
    public void testGetConflictRowRecordView() throws Exception {
        QueryRecordsRequest srcRequest = new QueryRecordsRequest("srcMha", "handleSql", new ArrayList<>(), new ArrayList<>(), 12);
        QueryRecordsRequest dstRequest = new QueryRecordsRequest("dstMha", "handleSql", new ArrayList<>(), new ArrayList<>(), 12);
        Mockito.when(conflictTrxLogTblDao.queryById(Mockito.anyLong())).thenReturn(buildConflictTrxLogTbls().get(0));
        Mockito.when(conflictRowsLogTblDao.queryById(Mockito.anyLong())).thenReturn(buildConflictRowsLogTbls().get(0));
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("srcMha"))).thenReturn(getMhaTbls().get(0));
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("dstMha"))).thenReturn(getMhaTbls().get(1));
        Mockito.when(drcBuildServiceV2.getDbReplicationView(Mockito.anyString(), Mockito.anyString())).thenReturn(getDbReplicationViews());
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Mockito.anyList())).thenReturn(getFilterMappings());
        Mockito.when(columnsFilterTblV2Dao.queryByIds(Mockito.anyList())).thenReturn(Lists.newArrayList(getColumnsFilterTbl()));
        Mockito.when(mysqlService.queryTableRecords(srcRequest)).thenReturn(getSrcResMap());
        Mockito.when(mysqlService.queryTableRecords(dstRequest)).thenReturn(getSrcResMap());

        ConflictCurrentRecordView result = conflictLogService.getConflictRowRecordView(1L, 12);
        Assert.assertTrue(result.isRecordIsEqual());

        Mockito.when(mysqlService.queryTableRecords(dstRequest)).thenReturn(getDstResMap());
        result = conflictLogService.getConflictRowRecordView(1L, 12);
        Assert.assertFalse(result.isRecordIsEqual());
    }

    @Test
    public void testCompareRowRecords() throws Exception {
        Mockito.when(conflictTrxLogTblDao.queryById(Mockito.anyLong())).thenReturn(buildConflictTrxLogTbls().get(0));
        Mockito.when(conflictRowsLogTblDao.queryById(Mockito.anyLong())).thenReturn(buildConflictRowsLogTbls().get(0));
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("srcMha"))).thenReturn(getMhaTbls().get(0));
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("dstMha"))).thenReturn(getMhaTbls().get(1));
        Mockito.when(drcBuildServiceV2.getDbReplicationView(Mockito.anyString(), Mockito.anyString())).thenReturn(getDbReplicationViews());
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Mockito.anyList())).thenReturn(getFilterMappings());
        Mockito.when(columnsFilterTblV2Dao.queryByIds(Mockito.anyList())).thenReturn(Lists.newArrayList(getColumnsFilterTbl()));
        Mockito.when(mysqlService.queryTableRecords(Mockito.any())).thenReturn(getSrcResMap());
        Mockito.when(mysqlService.queryTableRecords(Mockito.any())).thenReturn(getDstResMap());

        ConflictRowsRecordCompareView result = conflictLogService.compareRowRecords(Lists.newArrayList(1L));
        Assert.assertEquals(result.getRecordDetailList().size(), 1);
        Assert.assertTrue(CollectionUtils.isEmpty(result.getRowLogIds()));
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
        List<AviatorRegexFilter> filters = getConflictDbBlackListTbls().stream().map(tbl -> new AviatorRegexFilter(tbl.getDbFilter())).collect(Collectors.toList());
        Mockito.when(dbBlacklistCache.getDbBlacklistInCache()).thenReturn(filters);
        Mockito.when(consoleConfig.getConflictLogRecordSwitch()).thenReturn(false);
        ConflictTransactionLog trxLog = buildConflictTransactionLog();
        conflictLogService.insertConflictLog(Lists.newArrayList(trxLog));
        Mockito.verify(conflictTrxLogTblDao, Mockito.times(0)).batchInsertWithReturnId(Mockito.anyList());

        Mockito.when(consoleConfig.getConflictLogRecordSwitch()).thenReturn(true);
        Mockito.when(conflictTrxLogTblDao.batchInsertWithReturnId(Mockito.anyList())).thenReturn(buildConflictTrxLogTbls());
        Mockito.when(conflictRowsLogTblDao.insert(Mockito.anyList())).thenReturn(new int[1]);
        conflictLogService.insertConflictLog(Lists.newArrayList(trxLog));
    }

    @Test
    public void testGetConflictCurrentRecordView() throws Exception {
        Mockito.when(conflictTrxLogTblDao.queryById(Mockito.anyLong())).thenReturn(buildConflictTrxLogTbls().get(0));
        Mockito.when(conflictRowsLogTblDao.queryByTrxLogId(Mockito.anyLong())).thenReturn(buildConflictRowsLogTbls());
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("srcMha"))).thenReturn(getMhaTbls().get(0));
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("dstMha"))).thenReturn(getMhaTbls().get(1));
        Mockito.when(drcBuildServiceV2.getDbReplicationView(Mockito.anyString(), Mockito.anyString())).thenReturn(getDbReplicationViews());
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Mockito.anyList())).thenReturn(getFilterMappings());
        Mockito.when(columnsFilterTblV2Dao.queryByIds(Mockito.anyList())).thenReturn(Lists.newArrayList(getColumnsFilterTbl()));
        Mockito.when(mysqlService.queryTableRecords(Mockito.any())).thenReturn(getSrcResMap());
        Mockito.when(mysqlService.queryTableRecords(Mockito.any())).thenReturn(getDstResMap());

        ConflictCurrentRecordView result = conflictLogService.getConflictCurrentRecordView(1L, 12);
        Assert.assertTrue(result.isRecordIsEqual());
    }

    @Test
    public void testDeleteTrxLogs() throws Exception {
        Mockito.when(conflictTrxLogTblDao.queryByHandleTime(Mockito.anyLong(), Mockito.anyLong())).thenReturn(buildConflictTrxLogTbls());
        Mockito.when(conflictRowsLogTblDao.queryByTrxLogIds(Mockito.anyList())).thenReturn(buildConflictRowsLogTbls());
        Mockito.when(conflictTrxLogTblDao.batchDelete(Mockito.anyList())).thenReturn(new int[1]);
        Mockito.when(conflictRowsLogTblDao.batchDelete(Mockito.anyList())).thenReturn(new int[1]);

        long result = conflictLogService.deleteTrxLogs(0, System.currentTimeMillis());
        Assert.assertEquals(result, buildConflictRowsLogTbls().size());
    }

    @Test
    public void testDeleteTrxLogsByTime() throws Exception {
        Mockito.when(conflictTrxLogTblDao.batchDeleteByHandleTime(Mockito.anyLong(), Mockito.anyLong())).thenReturn(1);
        Mockito.when(conflictRowsLogTblDao.batchDeleteByHandleTime(Mockito.anyLong(), Mockito.anyLong())).thenReturn(1);

        Map<String, Integer> result = conflictLogService.deleteTrxLogsByTime(1L, 1L);
        Assert.assertNotNull(result);
    }

    @Test
    public void testGetConflictRowLogDetailView() throws Exception {
        Mockito.when(conflictRowsLogTblDao.queryByIds(Mockito.anyList())).thenReturn(buildConflictRowsLogTbls());
        Mockito.when(conflictTrxLogTblDao.queryByIds(Mockito.anyList())).thenReturn(buildConflictTrxLogTbls());
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("srcMha"))).thenReturn(getMhaTbls().get(0));
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("dstMha"))).thenReturn(getMhaTbls().get(1));
        Mockito.when(dcTblDao.queryById(Mockito.anyLong())).thenReturn(getDcTbls().get(0));
        ConflictTrxLogDetailView result = conflictLogService.getRowLogDetailView(Lists.newArrayList(1L));

        Assert.assertEquals(result.getRowsLogDetailViews().size(), 1);
    }

    @Test
    public void testGetConflictRowRecordView02() throws Exception {
        Mockito.when(conflictRowsLogTblDao.queryByIds(Mockito.anyList())).thenReturn(buildConflictRowsLogTbls());
        Mockito.when(conflictTrxLogTblDao.queryByIds(Mockito.anyList())).thenReturn(buildConflictTrxLogTbls());

        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("srcMha"))).thenReturn(getMhaTbls().get(0));
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("dstMha"))).thenReturn(getMhaTbls().get(1));
        Mockito.when(drcBuildServiceV2.getDbReplicationView(Mockito.anyString(), Mockito.anyString())).thenReturn(getDbReplicationViews());
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Mockito.anyList())).thenReturn(getFilterMappings());
        Mockito.when(columnsFilterTblV2Dao.queryByIds(Mockito.anyList())).thenReturn(Lists.newArrayList(getColumnsFilterTbl()));
        Mockito.when(mysqlService.queryTableRecords(Mockito.any())).thenReturn(getSrcResMap());

        ConflictCurrentRecordView result = conflictLogService.getConflictRowRecordView(Lists.newArrayList(1L));
        Assert.assertEquals(result.getSrcRecords().size(), 1);
        Assert.assertTrue(result.isRecordIsEqual());
    }

    @Test
    public void testCreateHandleSql() throws Exception {
        QueryRecordsRequest srcRequest = new QueryRecordsRequest("srcMha", "handleSql", Lists.newArrayList("datachange_lasttime"), Lists.newArrayList("id"), 12);
        QueryRecordsRequest dstRequest = new QueryRecordsRequest("dstMha", "handleSql", Lists.newArrayList("datachange_lasttime"), Lists.newArrayList("id"), 12);

        Mockito.when(conflictRowsLogTblDao.queryByIds(Mockito.anyList())).thenReturn(buildConflictRowsLogTbls());
        Mockito.when(conflictTrxLogTblDao.queryByIds(Mockito.anyList())).thenReturn(buildConflictTrxLogTbls());
        Mockito.when(drcBuildServiceV2.getDbReplicationView(Mockito.anyString(), Mockito.anyString())).thenReturn(getDbReplicationViews());
        Mockito.when(mysqlService.getFirstUniqueIndex(Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn("id");
        Mockito.when(mysqlService.queryTableRecords(srcRequest)).thenReturn(getSrcResMap());
        Mockito.when(mysqlService.queryTableRecords(dstRequest)).thenReturn(getDstResMap());
        Mockito.when(mysqlService.getAllOnUpdateColumns(Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn(Lists.newArrayList("datachange_lasttime"));

        ConflictAutoHandleParam param = new ConflictAutoHandleParam();
        param.setWriteSide(0);
        param.setRowLogIds(Lists.newArrayList(1L));

        List<ConflictAutoHandleView> result = conflictLogService.createHandleSql(param);
        Assert.assertEquals(result.size(), 1);

        Mockito.when(mysqlService.queryTableRecords(dstRequest)).thenReturn(getEmptyRecord());
        result = conflictLogService.createHandleSql(param);
        Assert.assertEquals(result.size(), 1);

        Mockito.when(mysqlService.queryTableRecords(srcRequest)).thenReturn(getEmptyRecord());
        Mockito.when(mysqlService.queryTableRecords(dstRequest)).thenReturn(getSrcResMap());
        result = conflictLogService.createHandleSql(param);
        Assert.assertEquals(result.size(), 1);
    }

    @Test
    public void testIsInBlackListWithCache() throws Exception {
        List<AviatorRegexFilter> filters = getConflictDbBlackListTbls().stream().map(tbl -> new AviatorRegexFilter(tbl.getDbFilter())).collect(Collectors.toList());
        Mockito.when(dbBlacklistCache.isInBlackListWithCache(anyString())).thenAnswer(
                p -> {
                    String fullName = p.getArgument(0);
                    for (AviatorRegexFilter filter : filters) {
                        if (filter.filter(fullName)) {
                            return true;
                        }
                    }
                    return false;
                }
        );
        Assert.assertTrue(conflictLogService.isInBlackListWithCache("db1", "table"));
        Assert.assertTrue(conflictLogService.isInBlackListWithCache("db2", "table"));
        Assert.assertFalse(conflictLogService.isInBlackListWithCache("db3", "table"));
    }

    @Test
    public void testCompareRowRecordsEqual() throws Exception {
        Mockito.when(conflictRowsLogTblDao.queryByIds(Mockito.anyList())).thenReturn(buildConflictRowsLogTbls());
        Mockito.when(conflictTrxLogTblDao.queryByIds(Mockito.anyList())).thenReturn(buildConflictTrxLogTbls());
        Mockito.when(drcBuildServiceV2.getDbReplicationView(Mockito.anyString(), Mockito.anyString())).thenReturn(getDbReplicationViews());
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Mockito.anyList())).thenReturn(getFilterMappings());
        Mockito.when(columnsFilterTblV2Dao.queryByIds(Mockito.anyList())).thenReturn(Lists.newArrayList(getColumnsFilterTbl()));
        Mockito.when(mysqlService.queryTableRecords(Mockito.any())).thenReturn(getSrcResMap());
        Mockito.when(mysqlService.queryTableRecords(Mockito.any())).thenReturn(getSrcResMap());

        List<ConflictRowRecordCompareEqualView> result = conflictLogService.compareRowRecordsEqual(Lists.newArrayList(1L));
        Assert.assertEquals(result.size(), 1);
        Assert.assertTrue(result.get(0).getRecordIsEqual());
    }

    @Test
    public void testGetRowsLogCountView() throws Exception {
        Mockito.when(conflictRowsLogTblDao.queryTopNDb(Mockito.anyLong(), Mockito.anyLong())).thenReturn(Lists.newArrayList(new ConflictRowsLogCount("db", "table", 1L, 1L, 1)));
        Mockito.when(conflictRowsLogTblDao.queryTopNDb(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyInt())).thenReturn(Lists.newArrayList(new ConflictRowsLogCount("db", "table", 1L, 1L, 1)));
        Mockito.when(conflictRowsLogTblDao.queryCount(Mockito.anyLong(), Mockito.anyLong())).thenReturn(1);
        Mockito.when(conflictRowsLogTblDao.queryCount(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyInt())).thenReturn(1);

        ConflictRowsLogCountView result = conflictLogService.getRowsLogCountView(1L, 2L);
        Assert.assertEquals(result.getDbCounts().size(), 1);
        Assert.assertEquals(result.getRollBackDbCounts().size(), 1);
        Assert.assertTrue(result.getTotalCount() == 1);
        Assert.assertTrue(result.getRollBackTotalCount() == 1);
    }

    @Test
    public void testGetConflictDbBlacklistView() throws Exception {
        Mockito.when(conflictDbBlackListTblDao.query(Mockito.any())).thenReturn(getConflictDbBlackListTbls());
        List<ConflictDbBlacklistView> result = conflictLogService.getConflictDbBlacklistView(new ConflictDbBlacklistQueryParam());
        Assert.assertEquals(result.size(), getConflictDbBlackListTbls().size());
    }

    private Map<String, Object> getEmptyRecord() {
        Map<String, Object> record = getSrcResMap();
        record.put("record", new ArrayList<>());
        return record;
    }

    private List<ConflictDbBlackListTbl> getConflictDbBlackListTbls() {
        ConflictDbBlackListTbl black1 = new ConflictDbBlackListTbl();
        black1.setDbFilter("db1\\..*");
        black1.setCreateTime(new Timestamp(System.currentTimeMillis()));
        black1.setType(0);
        black1.setId(1L);
        ConflictDbBlackListTbl black2 = new ConflictDbBlackListTbl();
        black2.setDbFilter("db2\\..*");
        black2.setCreateTime(new Timestamp(System.currentTimeMillis()));
        black2.setType(0);
        black2.setId(1L);
        return Lists.newArrayList(black1, black2);
    }

    private Map<String, Object> getSrcResMap() {
        Map<String, Object> res = new HashMap<>();
        res.put("tableName", "db.table");

        Map<String, Object> records = new HashMap<>();
        records.put("id", 1L);
        records.put("column", "a");
        records.put("datachange_lasttime", "time");
        records.put("drc_row_log_id", 1L);
        String a = "test";
        BigDecimal bigDecimal = new BigDecimal("1.010");
        records.put("a", CommonUtils.byteToHexString(a.getBytes(StandardCharsets.UTF_8)));
        records.put("b", null);
        records.put("bigDecimal", bigDecimal);

        Map<String, String> cellClassMap = new HashMap<>();
        cellClassMap.put("b", "cell-class-type");
        cellClassMap.put("a", "cell-class-type");
        records.put("cellClassName", cellClassMap);

        res.put("record", Lists.newArrayList(records));

        Map<String, Object> metaColumn = new HashMap<>();
        metaColumn.put("key", "column");
        res.put("metaColumn", Lists.newArrayList(metaColumn));

        Map<String, String> columnType = new HashMap<>();
        records.forEach((column, val) -> {
            if (val != null) {
                columnType.put(column, val.getClass().getName());
            }
        });

        res.put("columns", Lists.newArrayList(records.keySet()));
        res.put("columnType", columnType);
        return res;
    }

    private Map<String, Object> getDstResMap() {
        Map<String, Object> res = new HashMap<>();
        res.put("tableName", "db.table");

        Map<String, Object> records = new HashMap<>();
        records.put("id", 1L);
        records.put("column", "b");
        records.put("datachange_lasttime", "time");
        records.put("drc_row_log_id", 1L);

        String a = "test";
        BigDecimal bigDecimal = new BigDecimal("1.020");
        records.put("a", CommonUtils.byteToHexString(a.getBytes(StandardCharsets.UTF_8)));
        records.put("b", "b");
        records.put("bigDecimal", bigDecimal);

        Map<String, String> cellClassMap = new HashMap<>();
        cellClassMap.put("b", "cell-class-type");
        cellClassMap.put("a", "cell-class-type");
        records.put("cellClassName", cellClassMap);

        res.put("record", Lists.newArrayList(records));

        Map<String, Object> metaColumn = new HashMap<>();
        metaColumn.put("key", "column");
        res.put("metaColumn", Lists.newArrayList(metaColumn));

        Map<String, String> columnType = new HashMap<>();
        records.forEach((column, val) -> {
            if (val != null) {
                columnType.put(column, val.getClass().getName());
            }
        });
        res.put("columnType", columnType);
        res.put("columns", Lists.newArrayList());
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
