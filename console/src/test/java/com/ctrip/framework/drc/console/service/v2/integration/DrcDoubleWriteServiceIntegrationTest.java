package com.ctrip.framework.drc.console.service.v2.integration;

import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.ApplierGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.service.v2.AbstractIntegrationTest;
import com.ctrip.framework.drc.console.service.v2.DrcDoubleWriteService;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.platform.dal.dao.DalHints;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.AllTests.ciEndpoint;
import static com.ctrip.framework.drc.console.service.v2.MigrateEntityBuilder.*;

/**
 * Created by dengquanliang
 * 2023/7/18 18:04
 */
public class DrcDoubleWriteServiceIntegrationTest extends AbstractIntegrationTest {
    @Autowired
    private DrcDoubleWriteService drcDoubleWriteService;
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
    private MessengerTblDao messengerTblDao;
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

    @Before
    public void doBefore() throws Exception {
        List<ApplierGroupTbl> applierGroupTbls = getApplierGroupTbls().stream().filter(e -> e.getId().equals(200L)).collect(Collectors.toList());

        applierGroupTblDao.batchInsert(new DalHints().enableIdentityInsert(), applierGroupTbls);
        replicatorGroupTblDao.batchInsert(new DalHints().enableIdentityInsert(), getReplicatorGroupTbls());
        applierGroupTblV2Dao.batchInsert(new DalHints().enableIdentityInsert(), getApplierGroupTblV2s());
        applierTblV2Dao.batchInsert(new DalHints().enableIdentityInsert(), getApplierTblV2s());
        mhaDbMappingTblDao.batchInsert(new DalHints().enableIdentityInsert(), getMhaDbMappingTbls());
        mhaReplicationTblDao.batchInsert(new DalHints().enableIdentityInsert(), getMhaReplicationTbls());
        dbReplicationTblDao.batchInsert(new DalHints().enableIdentityInsert(), getDbReplicationTbls());


        rowsFilterTblDao.batchInsert(new DalHints().enableIdentityInsert(), getRowsFilterTbls());
        dbTblDao.batchInsert(new DalHints().enableIdentityInsert(), getDbTbls());
        dataMediaTblDao.batchInsert(new DalHints().enableIdentityInsert(), getDataMediaTbls());
        rowsFilterMappingTblDao.batchInsert(new DalHints().enableIdentityInsert(), getRowsFilterMapping());
        columnsFilterTblDao.batchInsert(new DalHints().enableIdentityInsert(), getColumnsFilterTbls());

        mhaGroupTblDao.batchInsert(new DalHints().enableIdentityInsert(), getMhaGroups());
        groupMappingTblDao.batchInsert(new DalHints().enableIdentityInsert(), getGroupMappings());
        mhaTblDao.batchInsert(new DalHints().enableIdentityInsert(), getMhaTbls());
        clusterMhaMapTblDao.batchInsert(new DalHints().enableIdentityInsert(), getClusterMhaMapTbl());
        clusterTblDao.batchInsert(new DalHints().enableIdentityInsert(), getClusterTbls());

        messengerGroupTblDao.insert(new DalHints().enableIdentityInsert(), getMessengerGroup());
        messengerTblDao.insert(new DalHints().enableIdentityInsert(), getMessenger());
        dataMediaPairTblDao.insert(new DalHints().enableIdentityInsert(), getDataMediaPairTbl());
    }

    @Test
    public void testQueryDbs() {
        List<String> dbs = MySqlUtils.queryDbsWithFilter(ciEndpoint, "");
    }

    @Test
    public void testBuildMha() throws Exception {
        drcDoubleWriteService.buildMha(200L);

        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryAll();
        Assert.assertEquals(mhaTblV2s.size(), 2);
        mhaTblV2s.forEach(System.out::println);
    }

    @Test
    public void testDeleteMhaReplicationConfig() throws Exception {
        drcDoubleWriteService.deleteMhaReplicationConfig(200L, 201L);

        List<ApplierGroupTblV2> applierGroupTblV2s = applierGroupTblV2Dao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierTblV2> applierTblV2s = applierTblV2Dao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryAll().stream()
                .filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode()) && e.getReplicationType() == ReplicationTypeEnum.DB_TO_DB.getType())
                .collect(Collectors.toList());

        Assert.assertEquals(applierGroupTblV2s.size(), 0);
        Assert.assertEquals(applierTblV2s.size(), 0);
        Assert.assertEquals(mhaReplicationTbls.size(), 0);
        Assert.assertEquals(dbReplicationTbls.size(), 0);
    }

    @Test
    public void testInsertRowsFilter() throws Exception {
        mhaTblV2Dao.batchInsert(new DalHints().enableIdentityInsert(), getMhaTblV2s());

        drcDoubleWriteService.insertRowsFilter(200L, 200L, 200L);
        List<RowsFilterTblV2> rowsFilters = rowsFilterTblV2Dao.queryAll();
        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao.queryAll();
        rowsFilters.forEach(System.out::println);
        dbReplicationFilterMappingTbls.forEach(System.out::println);
        Assert.assertEquals(rowsFilters.size(), 1);
        Assert.assertEquals(dbReplicationFilterMappingTbls.size(), 1);

        drcDoubleWriteService.insertRowsFilter(200L, 200L, 200L);
        rowsFilters = rowsFilterTblV2Dao.queryAll();
        dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao.queryAll();
        rowsFilters.forEach(System.out::println);
        dbReplicationFilterMappingTbls.forEach(System.out::println);
        Assert.assertEquals(rowsFilters.size(), 1);
        Assert.assertEquals(dbReplicationFilterMappingTbls.size(), 1);

        drcDoubleWriteService.deleteRowsFilter(200L);
        dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        Assert.assertEquals(dbReplicationFilterMappingTbls.size(), 0);
    }

    @Test
    public void testInsertColumnsFilter() throws Exception {
        mhaTblV2Dao.batchInsert(new DalHints().enableIdentityInsert(), getMhaTblV2s());

        drcDoubleWriteService.insertColumnsFilter(200L);
        List<ColumnsFilterTblV2> columnsFilters = columnFilterTblV2Dao.queryAll();
        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao.queryAll();
        columnsFilters.forEach(System.out::println);
        dbReplicationFilterMappingTbls.forEach(System.out::println);
        Assert.assertEquals(columnsFilters.size(), 1);
        Assert.assertEquals(dbReplicationFilterMappingTbls.size(), 1);

        drcDoubleWriteService.insertColumnsFilter(200L);
        columnsFilters = columnFilterTblV2Dao.queryAll();
        dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao.queryAll();
        columnsFilters.forEach(System.out::println);
        dbReplicationFilterMappingTbls.forEach(System.out::println);
        Assert.assertEquals(columnsFilters.size(), 1);
        Assert.assertEquals(dbReplicationFilterMappingTbls.size(), 1);

        drcDoubleWriteService.deleteColumnsFilter(200L);
        dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        Assert.assertEquals(dbReplicationFilterMappingTbls.size(), 0);
    }

    @Test
    public void testBuildMhaForMq() throws Exception {
        drcDoubleWriteService.buildMhaForMq(200L);

        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryAll();
        Assert.assertEquals(mhaTblV2s.size(), 1);
        mhaTblV2s.forEach(System.out::println);
    }

    @Test
    public void testDeleteDbReplicationForMq() throws Exception {
        mhaTblV2Dao.batchInsert(new DalHints().enableIdentityInsert(), getMhaTblV2s());
        drcDoubleWriteService.deleteDbReplicationForMq(200L);

        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryAll().stream()
                .filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode()) && e.getReplicationType() == ReplicationTypeEnum.DB_TO_MQ.getType())
                .collect(Collectors.toList());
        Assert.assertEquals(dbReplicationTbls.size(), 0);
    }

    @Test
    public void testConfigureDbReplicationForMq() throws Exception {
        mhaTblV2Dao.batchInsert(new DalHints().enableIdentityInsert(), getMhaTblV2s());
        drcDoubleWriteService.configureDbReplicationForMq(200L);

        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryAll().stream()
                .filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode()) && e.getReplicationType() == ReplicationTypeEnum.DB_TO_MQ.getType())
                .collect(Collectors.toList());
        List<MessengerFilterTbl> messengerFilterTbls = messengerFilterTblDao.queryAll();
        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTblList = dbReplicationFilterMappingTblDao.queryAll().stream()
                .filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode()))
                .collect(Collectors.toList());

        Assert.assertEquals(dbReplicationTbls.size(), 1);
        Assert.assertEquals(messengerFilterTbls.size(), 1);
        Assert.assertEquals(dbReplicationFilterMappingTblList.size(), 1);

        drcDoubleWriteService.configureDbReplicationForMq(200L);

        dbReplicationTbls = dbReplicationTblDao.queryAll().stream()
                .filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode()) && e.getReplicationType() == ReplicationTypeEnum.DB_TO_MQ.getType())
                .collect(Collectors.toList());
        messengerFilterTbls = messengerFilterTblDao.queryAll();
        dbReplicationFilterMappingTblList = dbReplicationFilterMappingTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());

        Assert.assertEquals(dbReplicationTbls.size(), 1);
        Assert.assertEquals(messengerFilterTbls.size(), 1);
        Assert.assertEquals(dbReplicationFilterMappingTblList.size(), 1);
    }

    @Test
    public void testInsertDbReplicationForMq() throws Exception {
        mhaTblV2Dao.batchInsert(new DalHints().enableIdentityInsert(), getMhaTblV2s());
        drcDoubleWriteService.insertDbReplicationForMq(200L);

        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryAll().stream()
                .filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode()) && e.getReplicationType() == ReplicationTypeEnum.DB_TO_MQ.getType())
                .collect(Collectors.toList());
        List<MessengerFilterTbl> messengerFilterTbls = messengerFilterTblDao.queryAll();
        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTblList = dbReplicationFilterMappingTblDao.queryAll();

        dbReplicationTbls.forEach(System.out::println);
        Assert.assertEquals(dbReplicationTbls.size(), 2);
        Assert.assertEquals(messengerFilterTbls.size(), 1);
        Assert.assertEquals(dbReplicationFilterMappingTblList.size(), 1);
    }
}
