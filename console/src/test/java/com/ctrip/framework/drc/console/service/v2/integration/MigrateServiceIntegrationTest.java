package com.ctrip.framework.drc.console.service.v2.integration;

import com.ctrip.framework.drc.console.AllTests;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.console.param.v2.MhaDbMappingMigrateParam;
import com.ctrip.framework.drc.console.service.v2.MigrateEntityBuilder;
import com.ctrip.framework.drc.console.service.v2.impl.MetaMigrateServiceImpl;
import com.ctrip.framework.drc.console.vo.api.MhaNameFilterVo;
import com.ctrip.framework.drc.console.vo.response.migrate.MigrateResult;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.platform.dal.dao.DalHints;
import com.google.common.collect.Lists;
import org.junit.*;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.sql.SQLException;
import java.util.List;

import static com.ctrip.framework.drc.console.utils.UTConstants.*;

/**
 * Created by dengquanliang
 * 2023/6/20 14:08
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@WebAppConfiguration
public class MigrateServiceIntegrationTest {

    private final static Logger logger = LoggerFactory.getLogger(MigrateServiceIntegrationTest.class);

    @Autowired
    private MetaMigrateServiceImpl migrationService;
    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;
    @Autowired
    private MhaTblDao mhaTblDao;
    @Autowired
    private MhaGroupTblDao mhaGroupTblDao;
    @Autowired
    private GroupMappingTblDao groupMappingTblDao;
    @Autowired
    private ClusterTblDao clusterTblDao;
    @Autowired
    private ClusterMhaMapTblDao clusterMhaMapTblDao;
    @Autowired
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Autowired
    private ApplierGroupTblV2Dao applierGroupTblV2Dao;
    @Autowired
    private ApplierGroupTblDao applierGroupTblDao;
    @Autowired
    private ApplierTblDao applierTblDao;
    @Autowired
    private ApplierTblV2Dao applierTblV2Dao;
    @Autowired
    private DbReplicationTblDao dbReplicationTblDao;
    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired
    private DbTblDao dbTblDao;
    @Autowired
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Autowired
    private DataMediaTblDao dataMediaTblDao;
    @Autowired
    private DataMediaPairTblDao dataMediaPairTblDao;
    @Autowired
    private MessengerGroupTblDao messengerGroupTblDao;
    @Autowired
    private MessengerFilterTblDao messengerFilterTblDao;
    @Autowired
    private RowsFilterMappingTblDao rowsFilterMappingTblDao;
    @Autowired
    private ColumnsFilterTblDao columnsFilterTblDao;
    @Autowired
    private ColumnsFilterTblV2Dao columnFilterTblV2Dao;
    @Autowired
    private RowsFilterTblDao rowsFilterTblDao;
    @Autowired
    private RowsFilterTblV2Dao rowsFilterTblV2Dao;
    @Autowired
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;

    private static WriteSqlOperatorWrapper writeSqlOperatorWrapper;
    private static List<String> dbs = Lists.newArrayList("cluster_tbl", "mha_tbl", "cluster_mha_map_tbl", "group_mapping_tbl",
            "mha_group_tbl", "db_tbl", "replicator_group_tbl", "applier_group_tbl", "applier_tbl", "rows_filter_mapping_tbl",
            "rows_filter_tbl", "data_media_tbl", "mha_tbl_v2", "mha_replication_tbl", "mha_db_mapping_tbl", "db_replication_tbl",
            "db_replication_filter_mapping_tbl", "columns_filter_tbl_v2", "columns_filter_tbl", "applier_group_tbl_v2", "applier_tbl_v2",
            "messenger_filter_tbl", "messenger_group_tbl", "dataMediaPair_tbl", "rows_filter_tbl_v2");

    @BeforeClass
    public static void setUp() throws Exception {
        AllTests.initTestDb();
        if (writeSqlOperatorWrapper == null) {
            writeSqlOperatorWrapper = new WriteSqlOperatorWrapper(new DefaultEndPoint(MYSQL_IP, META_DB_PORT, MYSQL_USER, null));
            try {
                writeSqlOperatorWrapper.initialize();
                writeSqlOperatorWrapper.start();
            } catch (Exception e) {
                logger.error("sqlOperatorWrapper initialize: ", e);
            }
        }

        truncateDbs();

    }

    @AfterClass
    public static void tearDown() {
    }

    @Before
    public void doBefore() throws Exception {
        initDb();
    }

    @After
    public void doAfter() throws Exception {
        truncateDbs();
//        mhaTblDao.batchInsert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getMhaTbls());
    }

    @Test
    public void testMigrateMhaTbl() throws Exception {
        clusterTblDao.batchInsert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getClusterTbls());
        clusterMhaMapTblDao.batchInsert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getClusterMhaMapTbl());

        MigrateResult result = migrationService.migrateMhaTbl();
        Assert.assertEquals(result.getInsertSize(), 2);
        Assert.assertEquals(result.getUpdateSize(), 0);
        Assert.assertEquals(result.getDeleteSize(), 0);

        result = migrationService.migrateMhaTbl();
        Assert.assertEquals(result.getInsertSize(), 0);
        Assert.assertEquals(result.getUpdateSize(), 2);
        Assert.assertEquals(result.getDeleteSize(), 2);

        List<MhaTblV2> tbls = mhaTblV2Dao.queryAll();
        Assert.assertEquals(tbls.size(), 2);
        tbls.forEach(System.out::println);
    }

    @Test
    public void testMigrateMhaReplication() throws Exception {
        MigrateResult result = migrationService.migrateMhaReplication();
        Assert.assertEquals(result.getInsertSize(), 2);
        Assert.assertEquals(result.getUpdateSize(), 0);
        Assert.assertEquals(result.getDeleteSize(), 0);

        result = migrationService.migrateMhaReplication();
        Assert.assertEquals(result.getInsertSize(), 2);
        Assert.assertEquals(result.getUpdateSize(), 0);
        Assert.assertEquals(result.getDeleteSize(), 2);

        List<MhaReplicationTbl> tbls = mhaReplicationTblDao.queryAll();
        Assert.assertEquals(tbls.size(), 2);
        tbls.forEach(System.out::println);
    }

    @Test
    public void testMigrateApplier() throws Exception {
        MigrateResult result = migrationService.migrateApplier();
        Assert.assertEquals(result.getInsertSize(), 2);
        Assert.assertEquals(result.getUpdateSize(), 0);
        Assert.assertEquals(result.getDeleteSize(), 0);

        result = migrationService.migrateApplier();
        Assert.assertEquals(result.getInsertSize(), 0);
        Assert.assertEquals(result.getUpdateSize(), 2);
        Assert.assertEquals(result.getDeleteSize(), 2);

        List<ApplierTblV2> tbls = applierTblV2Dao.queryAll();
        Assert.assertEquals(tbls.size(), 2);
        tbls.forEach(System.out::println);
    }

    @Test
    public void testMigrateApplierGroup() throws Exception {
        mhaReplicationTblDao.batchInsert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getMhaReplicationTbls());
        MigrateResult result = migrationService.migrateApplierGroup();
        Assert.assertEquals(result.getInsertSize(), 2);
        Assert.assertEquals(result.getUpdateSize(), 0);
        Assert.assertEquals(result.getDeleteSize(), 0);

        result = migrationService.migrateApplierGroup();
        Assert.assertEquals(result.getInsertSize(), 0);
        Assert.assertEquals(result.getUpdateSize(), 2);
        Assert.assertEquals(result.getDeleteSize(), 2);

        List<ApplierGroupTblV2> tbls = applierGroupTblV2Dao.queryAll();
        Assert.assertEquals(tbls.size(), 2);
        tbls.forEach(System.out::println);
    }

    @Test
    public void testMigrateColumnsFilter() throws Exception {
        MigrateResult result = migrationService.migrateColumnsFilter();
        Assert.assertEquals(result.getInsertSize(), 1);
        Assert.assertEquals(result.getUpdateSize(), 0);
        Assert.assertEquals(result.getDeleteSize(), 0);

        result = migrationService.migrateColumnsFilter();
        Assert.assertEquals(result.getInsertSize(), 1);
        Assert.assertEquals(result.getUpdateSize(), 0);
        Assert.assertEquals(result.getDeleteSize(), 1);

        List<ColumnsFilterTblV2> tbls = columnFilterTblV2Dao.queryAll();
        Assert.assertEquals(tbls.size(), 1);
        tbls.forEach(System.out::println);
    }

    @Test
    public void testMigrateRowsFilter() throws Exception {
        MigrateResult result = migrationService.migrateRowsFilter();
        Assert.assertEquals(result.getInsertSize(), 1);
        Assert.assertEquals(result.getUpdateSize(), 0);
        Assert.assertEquals(result.getDeleteSize(), 0);

        result = migrationService.migrateRowsFilter();
        Assert.assertEquals(result.getInsertSize(), 1);
        Assert.assertEquals(result.getUpdateSize(), 0);
        Assert.assertEquals(result.getDeleteSize(), 1);

        List<RowsFilterTblV2> tbls = rowsFilterTblV2Dao.queryAll();
        Assert.assertEquals(tbls.size(), 1);
        tbls.forEach(System.out::println);
    }

    @Test
    public void testMigrateMessengerFilter() throws Exception {
        MigrateResult result = migrationService.migrateMessengerFilter();
        Assert.assertEquals(result.getInsertSize(), 1);
        Assert.assertEquals(result.getUpdateSize(), 0);
        Assert.assertEquals(result.getDeleteSize(), 0);

        result = migrationService.migrateMessengerFilter();
        Assert.assertEquals(result.getInsertSize(), 1);
        Assert.assertEquals(result.getUpdateSize(), 0);
        Assert.assertEquals(result.getDeleteSize(), 1);

        List<MessengerFilterTbl> tbls = messengerFilterTblDao.queryAll();
        Assert.assertEquals(tbls.size(), 1);
        tbls.forEach(System.out::println);
    }

    @Test
    public void testMigrateDbReplicationFilterMapping() throws Exception {
        dbReplicationTblDao.batchInsert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getDbReplicationTbls());
        mhaDbMappingTblDao.batchInsert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getMhaDbMappingTbls());
        mhaReplicationTblDao.insert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getMhaReplicationTbl());
        applierGroupTblV2Dao.batchInsert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getApplierGroupTblV2s());

        dataMediaTblDao.batchInsert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getDataMediaTbls());
        rowsFilterMappingTblDao.batchInsert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getRowsFilterMapping());

        dbTblDao.batchInsert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getDbTbls());
        messengerGroupTblDao.insert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getMessengerGroup());
        messengerFilterTblDao.insert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getMessengerFilters());

        rowsFilterTblV2Dao.insert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getRowsFilterTblV2());
        columnFilterTblV2Dao.insert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getColumnsFilterTblV2());

        MigrateResult result = migrationService.migrateDbReplicationFilterMapping();
        Assert.assertEquals(result.getInsertSize(), 2);
        Assert.assertEquals(result.getUpdateSize(), 0);
        Assert.assertEquals(result.getDeleteSize(), 0);

        result = migrationService.migrateDbReplicationFilterMapping();
        Assert.assertEquals(result.getInsertSize(), 2);
        Assert.assertEquals(result.getUpdateSize(), 0);
        Assert.assertEquals(result.getDeleteSize(), 2);

        List<DbReplicationFilterMappingTbl> tbls = dbReplicationFilterMappingTblDao.queryAll();
        Assert.assertEquals(tbls.size(), 2);
        tbls.forEach(System.out::println);
    }

    @Test
    public void testManualMigrateVPCMhaDbMapping() throws Exception{
        mhaTblV2Dao.insert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getMhaTblV2());
        dbTblDao.batchInsert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getDbTbls());
        mhaDbMappingTblDao.batchInsert(MigrateEntityBuilder.getMhaDbMappingTbls());


        MigrateResult result = migrationService.manualMigrateVPCMhaDbMapping(new MhaDbMappingMigrateParam("mha", Lists.newArrayList("db200", "db201")));
        Assert.assertEquals(result.getInsertSize(), 1);

        MigrateResult result1 = migrationService.manualMigrateVPCMhaDbMapping(new MhaDbMappingMigrateParam("mha", Lists.newArrayList("db200", "db201")));
        Assert.assertEquals(result1.getInsertSize(), 0);

    }

    @Test
    public void testCheckVPCMha() throws Exception{
        mhaTblV2Dao.insert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getMhaTblV2());
        List<MhaNameFilterVo> result = migrationService.checkVPCMha(Lists.newArrayList("mha"));
        System.out.println(result);
        Assert.assertEquals(result.size(), 1);
    }

    private static void truncateDbs() {
        String TRUNCATE_TBL = "truncate table fxdrcmetadb.%s";
        for (String db : dbs) {
            String sql = String.format(TRUNCATE_TBL, db);
            GeneralSingleExecution execution = new GeneralSingleExecution(sql);
            try {
                writeSqlOperatorWrapper.write(execution);
            } catch (SQLException throwables) {
                logger.error("Failed truncte table : {}", db, throwables);
            }
        }
    }

    private void initDb() throws Exception {
        mhaTblDao.batchInsert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getMhaTbls());
        mhaGroupTblDao.batchInsert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getMhaGroups());
        groupMappingTblDao.batchInsert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getGroupMappings());
        replicatorGroupTblDao.batchInsert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getReplicatorGroupTbls());
        applierGroupTblDao.batchInsert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getApplierGroupTbls());
        applierTblDao.batchInsert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getApplierTbls());
        columnsFilterTblDao.batchInsert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getColumnsFilterTbls());
        dataMediaPairTblDao.insert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getDataMediaPairTbl());
        rowsFilterTblDao.batchInsert(new DalHints().enableIdentityInsert(), MigrateEntityBuilder.getRowsFilterTbls());
    }
}
