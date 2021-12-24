package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.DataConsistencyMonitorTbl;
import com.ctrip.framework.drc.console.dao.entity.DataInconsistencyHistoryTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.monitor.ConsistentMonitorContainer;
import com.ctrip.framework.drc.console.monitor.delay.config.ConsistencyMonitorConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.FullDataConsistencyCheckTestConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.FullDataConsistencyMonitorConfig;
import com.ctrip.framework.drc.console.service.monitor.impl.ConsistencyConsistencyMonitorServiceImpl;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.vo.MhaGroupPair;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.platform.dal.dao.DalQueryDao;
import com.ctrip.platform.dal.dao.helper.DalDefaultJpaMapper;
import org.junit.*;
import org.junit.runners.MethodSorters;
import org.mockito.*;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_OFF;
import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;

/**
 * Created by jixinwang on 2020/12/30
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ConsistencyConsistencyMonitorServiceImplTest {

    public static final String SHAOY = "shaoy";
    public static final String SHARB = "sharb";
    public static final String CLUSTER = "testCluster";
    public static final String MHAOY = "mhaoy";
    public static final String MHARB = "mharb";
    public static final String SCHEMA1 = "schema1";
    public static final String TABLE1 = "table1";
    public static final String SCHEMA2 = "schema2";
    public static final String TABLE2 = "table2";
    public static final String SCHEMA3 = "schema3";
    public static final String TABLE3 = "table3";
    public static final String SCHEMA4 = "schema4";
    public static final String TABLE4 = "table4";
    public static final String UUID = "uuid0";
    public static final String GTID = "uuid0:1-100";
    public static final String DB_IP = "10.0.0.1";
    public static final String REPLICATOR_IP = "100.0.0.1";
    public static final int DB_PORT = 3306;
    public static final int REPLICATOR_APPLIER_PORT = 8383;

    @InjectMocks
    private ConsistencyConsistencyMonitorServiceImpl consistencyMonitorServiceImpl = new ConsistencyConsistencyMonitorServiceImpl(new DataConsistencyMonitorTblDao(),
            new MhaTblDao(), new MhaGroupTblDao(), new MachineTblDao(), new DcTblDao(),
            new DalQueryDao("fxdrcmetadb_w"), new DalDefaultJpaMapper<>(DataInconsistencyHistoryTbl.class));

    @Mock
    private MetaInfoServiceImpl metaInfoService;

    @Mock
    private DefaultConsoleConfig defaultConsoleConfig;

    @Mock
    private MetaGenerator metaService;

    @Mock
    private DrcBuildServiceImpl drcBuildService;

    @Mock
    private DalUtils dalUtils = DalUtils.getInstance();

    @BeforeClass
    public static void initData() {
        try {
            ConsistencyConsistencyMonitorServiceImpl consistencyMonitorServiceImpl = new ConsistencyConsistencyMonitorServiceImpl(new DataConsistencyMonitorTblDao(),
                    new MhaTblDao(), new MhaGroupTblDao(), new MachineTblDao(), new DcTblDao(),
                    new DalQueryDao("fxdrcmetadb_w"), new DalDefaultJpaMapper<>(DataInconsistencyHistoryTbl.class));
            String getKeyValueSql = "insert into data_inconsistency_history_tbl(id, monitor_schema_name, monitor_table_name, monitor_table_key, monitor_table_key_value, mha_group_id, source_type, create_time) values(100, 'fxdrcmetadb', 'data_inconsistency_history_tbl', 'id', '100', 1, 2, '2021-02-22 17:22:59');";
            consistencyMonitorServiceImpl.executeCustomSql(getKeyValueSql);
            String insertMhaGroup = "insert into mha_group_tbl(id, drc_status, drc_establish_status, read_user, write_user, monitor_user) values(100, 1, 60, 'root', 'root', 'root');";
            consistencyMonitorServiceImpl.executeCustomSql(insertMhaGroup);
            String insertMha1 = "insert into mha_tbl(id, mha_name, mha_group_id, dc_id) values(100, 'test-consistency-1', 100, 1);";
            String insertMha2 = "insert into mha_tbl(id, mha_name, mha_group_id, dc_id) values(101, 'test-consistency-2', 100, 2);";
            consistencyMonitorServiceImpl.executeCustomSql(insertMha1);
            consistencyMonitorServiceImpl.executeCustomSql(insertMha2);
            String insertMachine1 = "insert into machine_tbl(ip, port, master, mha_id) values('127.0.0.1', 12345, 0, 100);";
            String insertMachine2 = "insert into machine_tbl(ip, port, master, mha_id) values('127.0.0.1', 12345, 1, 100);";
            String insertMachine3 = "insert into machine_tbl(ip, port, master, mha_id) values('127.0.0.1', 12345, 0, 101);";
            String insertMachine4 = "insert into machine_tbl(ip, port, master, mha_id) values('127.0.0.1', 12345, 1, 101);";
            consistencyMonitorServiceImpl.executeCustomSql(insertMachine1);
            consistencyMonitorServiceImpl.executeCustomSql(insertMachine2);
            consistencyMonitorServiceImpl.executeCustomSql(insertMachine3);
            consistencyMonitorServiceImpl.executeCustomSql(insertMachine4);
            String sql = "insert into data_inconsistency_history_tbl(monitor_schema_name, monitor_table_name, monitor_table_key, monitor_table_key_value, mha_group_id, source_type, create_time) values('fxdrcmetadb', 'data_inconsistency_history_tbl', 'id', '100', 1, 1, '2021-02-22 17:22:59');";
            consistencyMonitorServiceImpl.executeCustomSql(sql);

            DelayMonitorConfig delayMonitorConfig1 = new DelayMonitorConfig();
            delayMonitorConfig1.setTable("table1");
            delayMonitorConfig1.setKey("key1");
            delayMonitorConfig1.setOnUpdate("update1");
            consistencyMonitorServiceImpl.addDataConsistencyMonitor("fat-fx-drc1", "fat-fx-drc2", delayMonitorConfig1);
            DelayMonitorConfig delayMonitorConfig2 = new DelayMonitorConfig();
            delayMonitorConfig2.setTable("table2");
            delayMonitorConfig2.setKey("key2");
            delayMonitorConfig2.setOnUpdate("update2");
            consistencyMonitorServiceImpl.addDataConsistencyMonitor("fat-fx-drc1", "fat-fx-drc2", delayMonitorConfig2);

            String insertMonitorTable = "insert into data_consistency_monitor_tbl(id, mha_id, monitor_schema_name, monitor_table_name, monitor_table_key, monitor_table_on_update) values(100, 100, 'fxdrcmetadb', 'data_inconsistency_history_tbl', 'id', 'create_time');";
            consistencyMonitorServiceImpl.executeCustomSql(insertMonitorTable);
        } catch (SQLException e) {
        }
    }

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }


    public ConsistencyConsistencyMonitorServiceImplTest() throws SQLException {
    }

    @Test
    public void testAAOrderAddDataConsistencyMonitor() throws SQLException {
        List<DataConsistencyMonitorTbl> ret = consistencyMonitorServiceImpl.getAllDataConsistencyMonitor();
        Assert.assertNotEquals(0, ret.size());
        switchOnAllDataConsistencyMonitor();
    }

    @Test
    public void testABOrderGetDataConsistencyMonitor() throws SQLException {
        List<DataConsistencyMonitorTbl> ret = consistencyMonitorServiceImpl.getDataConsistencyMonitor("fat-fx-drc1", "fat-fx-drc2");
        Assert.assertNotEquals(0, ret.size());
        switchOnAllDataConsistencyMonitor();
    }

    @Test
    public void testACOrderDeleteDataConsistencyMonitor() throws SQLException {
        DelayMonitorConfig delayMonitorConfig3 = new DelayMonitorConfig();
        delayMonitorConfig3.setTable("table3");
        delayMonitorConfig3.setKey("key3");
        delayMonitorConfig3.setOnUpdate("update3");
        consistencyMonitorServiceImpl.addDataConsistencyMonitor("fat-fx-drc1", "fat-fx-drc2", delayMonitorConfig3);

        DataConsistencyMonitorTblDao dataConsistencyMonitorTblDao = new DataConsistencyMonitorTblDao();
        DataConsistencyMonitorTbl sample = new DataConsistencyMonitorTbl();
        sample.setMonitorTableName("table3");
        List<DataConsistencyMonitorTbl> ret3 = dataConsistencyMonitorTblDao.queryBy(sample);
        Assert.assertEquals(1, ret3.size());

        int id = ret3.get(0).getId();
        consistencyMonitorServiceImpl.deleteDataConsistencyMonitor(id);
        List<DataConsistencyMonitorTbl> ret4 = dataConsistencyMonitorTblDao.queryBy(sample);
        Assert.assertEquals(0, ret4.size());
        switchOnAllDataConsistencyMonitor();
    }

    private void switchOnAllDataConsistencyMonitor() throws SQLException {
        List<DataConsistencyMonitorTbl> allDataConsistencyMonitorTbls = consistencyMonitorServiceImpl.getAllDataConsistencyMonitor();
        allDataConsistencyMonitorTbls.forEach(dataConsistencyMonitorTbl -> dataConsistencyMonitorTbl.setMonitorSwitch(BooleanEnum.TRUE.getCode()));
        DalUtils.getInstance().getDataConsistencyMonitorTblDao().batchUpdate(allDataConsistencyMonitorTbls);
    }

    @Test
    public void testADOrderSwitchDataConsistencyMonitor() throws SQLException {
        ConsistencyMonitorConfig consistencyMonitorConfig1 = new ConsistencyMonitorConfig();
        ConsistencyMonitorConfig consistencyMonitorConfig2 = new ConsistencyMonitorConfig();
        ConsistencyMonitorConfig consistencyMonitorConfig3 = new ConsistencyMonitorConfig();
        ConsistencyMonitorConfig consistencyMonitorConfig4 = new ConsistencyMonitorConfig();
        consistencyMonitorConfig1.setSrcMha(MHAOY);
        consistencyMonitorConfig1.setDstMha(MHARB);
        consistencyMonitorConfig1.setSchema(SCHEMA1);
        consistencyMonitorConfig1.setTable(TABLE1);
        consistencyMonitorConfig2.setSrcMha(MHAOY);
        consistencyMonitorConfig2.setDstMha(MHARB);
        consistencyMonitorConfig2.setSchema(SCHEMA2);
        consistencyMonitorConfig2.setTable(TABLE2);
        consistencyMonitorConfig3.setSrcMha(MHAOY);
        consistencyMonitorConfig3.setDstMha(MHARB);
        consistencyMonitorConfig3.setSchema(SCHEMA3);
        consistencyMonitorConfig3.setTable(TABLE3);
        consistencyMonitorConfig4.setSrcMha(MHAOY);
        consistencyMonitorConfig4.setDstMha(MHARB);
        consistencyMonitorConfig4.setSchema(SCHEMA4);
        consistencyMonitorConfig4.setTable(TABLE4);
        List<ConsistencyMonitorConfig> consistencyMonitorConfigs = Arrays.asList(consistencyMonitorConfig1, consistencyMonitorConfig2, consistencyMonitorConfig3, consistencyMonitorConfig4);

        DataConsistencyMonitorTbl dataConsistencyMonitorTbl2 = new DataConsistencyMonitorTbl();
        DataConsistencyMonitorTbl dataConsistencyMonitorTbl3 = new DataConsistencyMonitorTbl();
        DataConsistencyMonitorTbl dataConsistencyMonitorTbl4 = new DataConsistencyMonitorTbl();
        dataConsistencyMonitorTbl2.setMhaId(1);
        dataConsistencyMonitorTbl2.setMonitorSchemaName(SCHEMA2);
        dataConsistencyMonitorTbl2.setMonitorTableName(TABLE2);
        dataConsistencyMonitorTbl2.setMonitorSwitch(1);
        dataConsistencyMonitorTbl3.setMhaId(2);
        dataConsistencyMonitorTbl3.setMonitorSchemaName(SCHEMA3);
        dataConsistencyMonitorTbl3.setMonitorTableName(TABLE3);
        dataConsistencyMonitorTbl3.setMonitorSwitch(0);
        dataConsistencyMonitorTbl4.setMhaId(1);
        dataConsistencyMonitorTbl4.setMonitorSchemaName(SCHEMA4);
        dataConsistencyMonitorTbl4.setMonitorTableName(TABLE4);
        dataConsistencyMonitorTbl4.setMonitorSwitch(0);
        Mockito.when(metaService.getDataConsistencyMonitorTbls()).thenReturn(Arrays.asList(dataConsistencyMonitorTbl2, dataConsistencyMonitorTbl3, dataConsistencyMonitorTbl4));
        Mockito.when(metaInfoService.getMhaIds(Arrays.asList(MHAOY, MHARB))).thenReturn(Arrays.asList(1L, 2L));
        Mockito.when(dalUtils.updateDataConsistencyMonitor(dataConsistencyMonitorTbl3)).thenReturn(1);
        Mockito.when(dalUtils.updateDataConsistencyMonitor(dataConsistencyMonitorTbl4)).thenThrow(new SQLException());
        Map<String, Boolean> map = consistencyMonitorServiceImpl.switchDataConsistencyMonitor(consistencyMonitorConfigs, SWITCH_STATUS_ON);
        Assert.assertEquals(4, map.size());
        System.out.println(map);
        Assert.assertEquals(false, map.get(consistencyMonitorConfig1.uniqKey()));
        Assert.assertEquals(true, map.get(consistencyMonitorConfig2.uniqKey()));
        Assert.assertEquals(true, map.get(consistencyMonitorConfig3.uniqKey()));
        Assert.assertEquals(false, map.get(consistencyMonitorConfig4.uniqKey()));

        Mockito.when(metaService.getDataConsistencyMonitorTbls()).thenThrow(new SQLException());
        map = consistencyMonitorServiceImpl.switchDataConsistencyMonitor(consistencyMonitorConfigs, SWITCH_STATUS_ON);
        Assert.assertEquals(0, map.size());
    }

    @Test
    public void testAEOrderSwitchUnitVerification() throws Exception {
        String MHA1 = "mha1";
        String MHA2 = "mha2";
        MhaGroupTbl mhaGroupTbl = new MhaGroupTbl();
        mhaGroupTbl.setId(1L);
        mhaGroupTbl.setUnitVerificationSwitch(1);
        Mockito.doReturn(mhaGroupTbl).when(metaInfoService).getMhaGroup(MHA1, MHA2);

        MhaGroupPair mhaGroupPair = new MhaGroupPair();
        mhaGroupPair.setSrcMha(MHA1);
        mhaGroupPair.setDestMha(MHA2);
        boolean res = consistencyMonitorServiceImpl.switchUnitVerification(mhaGroupPair, SWITCH_STATUS_ON);
        Assert.assertTrue(res);

        Mockito.doReturn(1).when(dalUtils).updateMhaGroup(mhaGroupTbl);
        res = consistencyMonitorServiceImpl.switchUnitVerification(mhaGroupPair, SWITCH_STATUS_OFF);
        Assert.assertTrue(res);
    }

    @Test
    public void testAFOrderGetInconsistencyHistorySql() {
        String sql = consistencyMonitorServiceImpl.getCountInconsistencyHistorySql("testDbName", "testTableName", "testStartTime", "testndTime");
        Assert.assertNotNull(sql);

        sql = consistencyMonitorServiceImpl.getSelectInconsistencyHistorySql(0,10,"testDbName", "testTableName", "testStartTime", "testndTime");
        Assert.assertNotNull(sql);
    }

    @Test
    public void testAGOrderExecuteCustomSql() {
        String sql = "insert into data_inconsistency_history_tbl(monitor_schema_name) values('testDb');";
        boolean result = true;
        try {
            consistencyMonitorServiceImpl.executeCustomSql(sql);
        } catch (SQLException e) {
            result = false;
        }
        Assert.assertEquals(true, result);
    }

    @Test
    public void testAHOrderGetCurrentInconsistencyRecord() throws SQLException {
        Map<String, Object> fullInconsistencyRecordResult = consistencyMonitorServiceImpl.getCurrentFullInconsistencyRecord("test-consistency-1", "test-consistency-2", "fxdrcmetadb", "data_inconsistency_history_tbl", "id", "2021-02-22 17:22:59");
        int mhaACurrentResultList1 = ((List) fullInconsistencyRecordResult.get("mhaACurrentResultList")).size();
        int mhaBCurrentResultList1 = ((List) fullInconsistencyRecordResult.get("mhaACurrentResultList")).size();
        Assert.assertEquals(1, mhaACurrentResultList1);
        Assert.assertEquals(1, mhaBCurrentResultList1);

        Map<String, Object> incrementInconsistencyRecordResult = consistencyMonitorServiceImpl.getCurrentIncrementInconsistencyRecord(100, "fxdrcmetadb", "data_inconsistency_history_tbl", "id", "100");
        int mhaACurrentResultList2 = ((List) incrementInconsistencyRecordResult.get("mhaACurrentResultList")).size();
        int mhaBCurrentResultList2 = ((List) incrementInconsistencyRecordResult.get("mhaACurrentResultList")).size();
        Assert.assertEquals(1, mhaACurrentResultList2);
        Assert.assertEquals(1, mhaBCurrentResultList2);
    }

    @Test
    public void testAIOrderGetIncrementInconsistencyHistory() throws SQLException {
        List<DataInconsistencyHistoryTbl> result = consistencyMonitorServiceImpl.getIncrementInconsistencyHistory(1, 10, "fxdrcmetadb", "data_inconsistency_history_tbl", "", "");
        Assert.assertNotEquals(0, result.size());
    }

    @Test
    public void testAJOrderGetIncrementInconsistencyHistoryCount() throws SQLException {
        long count = consistencyMonitorServiceImpl.getIncrementInconsistencyHistoryCount("fxdrcmetadb", "data_inconsistency_history_tbl", "", "");
        Assert.assertNotEquals(0, count);
    }

    @Test
    public void testAKOrderHandleInconsistency() throws SQLException {
        Map<String, String> updateInfo = new HashMap<>();
        String mhaName = "test-consistency-1";
        String sql = "insert into fxdrcmetadb.data_inconsistency_history_tbl(monitor_schema_name, monitor_table_name, monitor_table_key, monitor_table_key_value, mha_group_id, source_type, create_time) values('testName', 'testTable', 'id', '1', 1, 2, '2021-02-22 17:22:59');";
        updateInfo.put("mhaName", mhaName);
        updateInfo.put("sql", sql);
        consistencyMonitorServiceImpl.handleInconsistency(updateInfo);
    }

    @Test
    public void testALOrderAddFullDataConsistencyMonitor() throws Exception {
        FullDataConsistencyMonitorConfig config = new FullDataConsistencyMonitorConfig();
        config.setMhaAName("test-consistency-1");
        config.setMhaBName("test-consistency-2");
        config.setSchema("fxdrcmetadb");
        config.setTable("data_inconsistency_history_tbl");
        config.setTableId(100);
        config.setKey("id");
        config.setOnUpdate("create_time");
        config.setEndTimeStamp("2021-02-22 17:23:59");
        ConsistentMonitorContainer consistentMonitorContainer = new ConsistentMonitorContainer();
        consistentMonitorContainer.addFullDataConsistencyCheck(config);
    }

    @Test
    public void testAMOrderAddUnitVerification() throws SQLException {
        // test 0
        Mockito.doThrow(new SQLException()).when(metaInfoService).getMachines(Mockito.anyLong());
        Assert.assertFalse(consistencyMonitorServiceImpl.addUnitVerification(1L));

        // test 1
        List<DBInfo> dbInfos = new ArrayList<>() {{
            add(new DBInfo());
            add(new DBInfo());
        }};
        dbInfos.forEach(dbInfo -> {
            dbInfo.setIdc(SHAOY);
            dbInfo.setUuid(UUID);
            dbInfo.setMhaName(MHAOY);
            dbInfo.setIp(DB_IP);
            dbInfo.setPort(DB_PORT);
            dbInfo.setCluster(CLUSTER);
        });
        Mockito.doReturn(dbInfos).when(metaInfoService).getMachines(Mockito.anyLong());
        Assert.assertTrue(consistencyMonitorServiceImpl.addUnitVerification(1L));

        // test 2
        MhaTbl mhaTbl = new MhaTbl();
        mhaTbl.setMhaName(MHAOY);
        Map<String, MhaTbl> mhaTblMap = new HashMap<>() {{
            put(SHAOY, mhaTbl);
        }};
        Mockito.doReturn(mhaTblMap).when(metaInfoService).getMhaTblMap(Mockito.anyLong());

        Mockito.doReturn(CLUSTER).when(metaInfoService).getCluster(Mockito.anyString());
        Mockito.doReturn(GTID).when(drcBuildService).getGtidInit(Mockito.any());

        InstanceInfo info = new InstanceInfo();
        info.setIp(REPLICATOR_IP);
        info.setPort(REPLICATOR_APPLIER_PORT);
        info.setMhaName(MHAOY);
        info.setCluster(CLUSTER);
        info.setIdc(SHAOY);
        Mockito.doReturn(info).when(metaInfoService).getReplicator(Mockito.any());

        Map<String, String> uidMap = new HashMap<>() {{
            put("`testdb`.`testtable`", "uid");
        }};
        Mockito.doReturn(uidMap).when(metaInfoService).getUidMap(Mockito.anyString(), Mockito.anyString());

        Map<String, Integer> ucsStrategyIdMap = new HashMap<>() {{
            put("testdb", 1);
        }};
        Mockito.doReturn(ucsStrategyIdMap).when(metaInfoService).getUcsStrategyIdMap(Mockito.anyString(), Mockito.anyString());

        Mockito.doReturn("http://100.100.0.1:8080").when(defaultConsoleConfig).getValidationDomain(Mockito.anyString());

        try (MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when((MockedStatic.Verification) HttpUtils.put(Mockito.anyString(), Mockito.any(), ApiResult.class)).thenReturn(ApiResult.getSuccessInstance(""));
            Assert.assertTrue(consistencyMonitorServiceImpl.addUnitVerification(1L));
        } catch (Exception e) {
            e.printStackTrace();
        }
        try (MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when((MockedStatic.Verification) HttpUtils.put(Mockito.anyString(), Mockito.any(), ApiResult.class)).thenReturn(ApiResult.getFailInstance(""));
            Assert.assertFalse(consistencyMonitorServiceImpl.addUnitVerification(1L));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testANOrderDeleteUnitVerification() throws SQLException {
        // test 0
        Mockito.doThrow(new SQLException()).when(metaInfoService).getMhaTblMap(Mockito.anyLong());
        Assert.assertFalse(consistencyMonitorServiceImpl.deleteUnitVerification(1L));

        MhaTbl mhaTbl = new MhaTbl();
        mhaTbl.setMhaName(MHAOY);
        Map<String, MhaTbl> mhaTblMap = new HashMap<>() {{
            put(SHAOY, mhaTbl);
        }};
        Mockito.doReturn(mhaTblMap).when(metaInfoService).getMhaTblMap(Mockito.anyLong());

        Mockito.doReturn(CLUSTER).when(metaInfoService).getCluster(Mockito.anyString());
        Mockito.doReturn(CLUSTER).when(metaInfoService).getCluster(Mockito.anyString());

        try (MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when((MockedStatic.Verification) HttpUtils.delete(Mockito.anyString(), ApiResult.class)).thenReturn(ApiResult.getFailInstance(""));
            Assert.assertFalse(consistencyMonitorServiceImpl.addUnitVerification(1L));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try (MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when((MockedStatic.Verification) HttpUtils.delete(Mockito.anyString(), ApiResult.class)).thenReturn(ApiResult.getSuccessInstance(""));
            Assert.assertTrue(consistencyMonitorServiceImpl.addUnitVerification(1L));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testAOOrderGetCurrentFullInconsistencyRecordForTest() throws Exception {
        // addFirst
        FullDataConsistencyCheckTestConfig testConfig = new FullDataConsistencyCheckTestConfig();

        testConfig.setIpA("127.0.0.1");
        testConfig.setPortA(12345);
        testConfig.setUserA("root");
        testConfig.setPasswordA(null);
        testConfig.setIpB("127.0.0.1");
        testConfig.setPortB(13306);
        testConfig.setUserB("root");
        testConfig.setPasswordB(null);
        testConfig.setSchema("fxdrcmetadb");
        testConfig.setTable("data_inconsistency_history_tbl");
        testConfig.setKey("id");
        testConfig.setOnUpdate("datachange_lasttime");
        testConfig.setEndTimeStamp("2021-09-06 11:00:00");


        ConsistentMonitorContainer consistentMonitorContainer = new ConsistentMonitorContainer();
        consistentMonitorContainer.initDaos();
        consistentMonitorContainer.addFullDataConsistencyCheckForTest(testConfig);

        //getStatus
        Map<String, Object> checkStatus = consistencyMonitorServiceImpl.getFullDataCheckStatusForTest(testConfig.getSchema(), testConfig.getTable(), testConfig.getKey());
        Timestamp checkTime = (Timestamp) checkStatus.get("checkTime");
        Integer checkStatusCode = (Integer) checkStatus.get("checkStatus");
        Assert.assertNotEquals(checkStatusCode, Integer.valueOf(0));

        //getRecord Use
        String checkTime1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(checkTime);
        consistencyMonitorServiceImpl.getCurrentFullInconsistencyRecordForTest(testConfig, checkTime1);


    }
}
