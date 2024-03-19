package com.ctrip.framework.drc.console.service.v2.dbmigration.impl;

import static com.ctrip.framework.drc.console.enums.ReplicationTypeEnum.DB_TO_DB;
import static com.ctrip.framework.drc.console.enums.ReplicationTypeEnum.DB_TO_MQ;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.MessengerGroupTblDao;
import com.ctrip.framework.drc.console.dao.MessengerTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorGroupTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplierGroupTblV2;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.entity.v2.MigrationTaskTbl;
import com.ctrip.framework.drc.console.dao.v2.ApplierGroupTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.ApplierTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.MigrationTaskTblDao;
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam;
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam.MigrateMhaInfo;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.MigrationStatusEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.CacheMetaService;
import com.ctrip.framework.drc.console.service.v2.DrcBuildServiceV2;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaDbMappingService;
import com.ctrip.framework.drc.console.service.v2.MhaDbReplicationService;
import com.ctrip.framework.drc.console.service.v2.MockEntityBuilder;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.service.v2.impl.MetaGeneratorV3;
import com.ctrip.framework.drc.core.config.RegionConfig;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class DbMigrationServiceImplTest {
    
    @InjectMocks
    private DbMigrationServiceImpl dbMigrationService;
    @Mock
    private MetaInfoServiceV2 metaInfoServiceV2;
    @Mock
    private MachineTblDao machineTblDao;
    @Mock
    private MigrationTaskTblDao migrationTaskTblDao;
    @Mock
    private MhaTblV2Dao mhaTblV2Dao;
    @Mock
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Mock
    private ApplierGroupTblV2Dao applierGroupTblV2Dao;
    @Mock
    private ApplierTblV2Dao applierTblV2Dao;
    @Mock
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Mock
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Mock
    private DbReplicationTblDao dbReplicationTblDao;
    @Mock
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Mock
    private DbTblDao dbTblDao;
    @Mock
    private MessengerGroupTblDao messengerGroupTblDao;
    @Mock
    private MessengerTblDao messengerTblDao;
    @Mock
    private MetaGeneratorV3 metaGeneratorV3;
    @Mock
    private DcTblDao dcTblDao;
    @Mock
    private MhaDbMappingService mhaDbMappingService;
    @Mock
    private DrcBuildServiceV2 drcBuildServiceV2;
    @Mock
    private MysqlServiceV2 mysqlServiceV2;
    @Mock
    private RegionConfig regionConfig;
    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private MhaDbReplicationService mhaDbReplicationService;
    @Mock
    private CacheMetaService cacheMetaService;


    // init tblEntity  mhaTbls & mhaReplicationTbls & dbTbls & mhaDbMappingTbls & dbReplicationTbls & filterMapping
    MhaTblV2 mha1 = MockEntityBuilder.buildMhaTblV2(1L,"mha1",1L);
    MhaTblV2 mha2 = MockEntityBuilder.buildMhaTblV2(2L,"mha2",1L);
    MhaTblV2 mha3 = MockEntityBuilder.buildMhaTblV2(3L,"mha3",1L);
    MhaTblV2 mha4 = MockEntityBuilder.buildMhaTblV2(4L,"mha4",1L);

    MhaReplicationTbl mhaReplication1_3 = MockEntityBuilder.buildMhaReplicationTbl(1L,mha1,mha3);
    MhaReplicationTbl mhaReplication3_1 = MockEntityBuilder.buildMhaReplicationTbl(2L,mha3,mha1);
    MhaReplicationTbl mhaReplication2_3 = MockEntityBuilder.buildMhaReplicationTbl(3L,mha2,mha3);
    MhaReplicationTbl mhaReplication3_2 = MockEntityBuilder.buildMhaReplicationTbl(4L,mha3,mha2);
    MhaReplicationTbl mhaReplication4_1 = MockEntityBuilder.buildMhaReplicationTbl(5L,mha4,mha1);

    MessengerGroupTbl mha1MessengerGroup = MockEntityBuilder.buildMessengerGroupTbl(1L, mha1.getId());

    DbTbl db1 = MockEntityBuilder.buildDbTbl(1L, "db1");
    DbTbl db2 = MockEntityBuilder.buildDbTbl(2L, "db2");
    DbTbl db3 = MockEntityBuilder.buildDbTbl(3L, "db3");

    
    
    MhaDbMappingTbl mha1Db1Mapping = MockEntityBuilder.buildMhaDbMappingTbl(1L, mha1, db1);
    MhaDbMappingTbl mha1Db2Mapping = MockEntityBuilder.buildMhaDbMappingTbl(2L, mha1, db2);
    MhaDbMappingTbl mha3Db1Mapping = MockEntityBuilder.buildMhaDbMappingTbl(3L, mha3, db1);
    MhaDbMappingTbl mha3Db2Mapping = MockEntityBuilder.buildMhaDbMappingTbl(4L, mha3, db2);
    MhaDbMappingTbl mha4Db2Mapping = MockEntityBuilder.buildMhaDbMappingTbl(5L, mha4, db2);
    MhaDbMappingTbl mha2Db1Mapping = MockEntityBuilder.buildMhaDbMappingTbl(6L, mha2, db1);
    MhaDbMappingTbl mha2Db2Mapping = MockEntityBuilder.buildMhaDbMappingTbl(7L, mha2, db2);
    MhaDbMappingTbl mha1Db3Mapping = MockEntityBuilder.buildMhaDbMappingTbl(8L, mha1, db3);
    MhaDbMappingTbl mha2Db3Mapping = MockEntityBuilder.buildMhaDbMappingTbl(9L, mha2, db3);
    MhaDbMappingTbl mha3Db3Mapping = MockEntityBuilder.buildMhaDbMappingTbl(10L, mha3, db3);
    MhaDbMappingTbl mha4Db3Mapping = MockEntityBuilder.buildMhaDbMappingTbl(11L, mha4, db3);
    
    
    DbReplicationTbl db1ReplicaInMha1_3 = MockEntityBuilder.buildDbReplicationTbl(1L, mha1Db1Mapping, mha3Db1Mapping,0);
    DbReplicationTbl db1ReplicaInMha3_1 = MockEntityBuilder.buildDbReplicationTbl(2L, mha3Db1Mapping, mha1Db1Mapping,0);
    DbReplicationTbl db1MqReplicaInMha1 = MockEntityBuilder.buildDbReplicationTbl(3L, mha1Db1Mapping, null,1);
    DbReplicationTbl db2ReplicaInMha4_1 = MockEntityBuilder.buildDbReplicationTbl(5L, mha4Db2Mapping, mha1Db2Mapping,0);
    DbReplicationTbl db3ReplicaInMha1_3 = MockEntityBuilder.buildDbReplicationTbl(6L, mha1Db3Mapping, mha3Db3Mapping,0);
    DbReplicationTbl db3MqReplicaInMha1 = MockEntityBuilder.buildDbReplicationTbl(7L, mha1Db3Mapping, null,1);

    DbReplicationFilterMappingTbl rowsFilterRule1 = MockEntityBuilder.buildDbReplicationFilterMappingTbl(1L,db1ReplicaInMha1_3,1L,null,null);
    DbReplicationFilterMappingTbl columnsFilterRule2 = MockEntityBuilder.buildDbReplicationFilterMappingTbl(2L,db1ReplicaInMha3_1,null,1L,null);
    DbReplicationFilterMappingTbl mqFilterRule3 = MockEntityBuilder.buildDbReplicationFilterMappingTbl(3L,db1MqReplicaInMha1,null,null,1L);
    DbReplicationFilterMappingTbl mqFilterRule4 = MockEntityBuilder.buildDbReplicationFilterMappingTbl(3L,db3MqReplicaInMha1,null,null,1L);
    
    MigrationTaskTbl migrationTaskTbl = MockEntityBuilder.buildMigrationTaskTbl(1L,"mha1","mha2","[\"db1\",\"db2\"]","drctest");

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        Mockito.when(migrationTaskTblDao.update(Mockito.any(MigrationTaskTbl.class))).thenReturn(1);
        Mockito.when(mhaTblV2Dao.update(Mockito.any(MhaTblV2.class))).thenReturn(1);
        Mockito.when(cacheMetaService.refreshMetaCache()).thenReturn(true);
    }

    @Test
    public void testDbMigrationCheckAndCreateTask() throws SQLException {
        DbMigrationParam dbMigrationParam = mockDbMigrationParam();
        MigrationTaskTbl existedTask = MockEntityBuilder.buildMigrationTaskTbl(1L, "mha1", "mha2",
                "[\"db1\",\"db2\"]", "operator");
        existedTask.setOldMhaDba("mha1");
        existedTask.setNewMhaDba("mha2");
        existedTask.setStatus(MigrationStatusEnum.PRE_STARTED.getStatus());
        Mockito.when(migrationTaskTblDao.queryByOldMhaDBA(Mockito.anyString())).thenReturn(Lists.newArrayList(existedTask));
        Mockito.when(consoleConfig.getLocalConfigCloudDc()).thenReturn(Sets.newHashSet());
        Pair<String, Long> stringLongPair1 = dbMigrationService.dbMigrationCheckAndCreateTask(dbMigrationParam);
        Assert.assertEquals(1L,stringLongPair1.getRight().longValue());

        Mockito.when(migrationTaskTblDao.queryByOldMhaDBA(Mockito.anyString())).thenReturn(Lists.newArrayList());
        // normal case
        mockMigrateDbsReplicationInfos();
        Pair<String, Long> stringLongPair = dbMigrationService.dbMigrationCheckAndCreateTask(dbMigrationParam);
        Assert.assertEquals(1L,stringLongPair.getRight().longValue());
        Mockito.verify(drcBuildServiceV2,Mockito.times(1)).syncMhaInfoFormDbaApi(Mockito.eq("mha2"));
        Mockito.verify(mhaTblV2Dao,Mockito.times(1)).queryByPk(Mockito.eq(1L));

        // check case1:migrate dbs effect multi mha-Replication in same region is not allowed;
        try {
            mockDbMigrationCheckForbiddenCase1();
            dbMigrationService.dbMigrationCheckAndCreateTask(dbMigrationParam);
        } catch (ConsoleException e) {
            Assert.assertEquals("region1: multi mhaTbs in drcReplication, please check! mha: mha4,mha3",e.getMessage());
        }
        
        // check case2:
        try {
            mockMigrateDbsReplicationInfos();
            mockDbMigrationCheckForbiddenCase2();
            dbMigrationService.dbMigrationCheckAndCreateTask(dbMigrationParam);
        } catch (ConsoleException e) {
            Assert.assertEquals("newMha and oldMha have common mha in Replication, please check! commomMhas: mha3",e.getMessage());
        }
    }

    @Test
    public void testPreStartDbMigrationTask() throws Exception {
        Mockito.when(migrationTaskTblDao.queryByPk(Mockito.eq(migrationTaskTbl.getId()))).thenReturn(migrationTaskTbl);
        Mockito.when(consoleConfig.getConfgiCheckSwitch()).thenReturn(true);
        
        try {
            migrationTaskTbl.setStatus(MigrationStatusEnum.STARTING.getStatus());
            dbMigrationService.preStartDbMigrationTask(migrationTaskTbl.getId());
        } catch (ConsoleException e) {
            Assert.assertEquals("task status is not INIT, can not exStart! taskId: 1",e.getMessage());
        }
        
        try {
            migrationTaskTbl.setStatus(MigrationStatusEnum.INIT.getStatus());
            mockConfigNotEqual();
            dbMigrationService.preStartDbMigrationTask(migrationTaskTbl.getId());
        } catch (ConsoleException e) {
            Assert.assertTrue(e.getMessage().contains("MhaConfigs not equals!"));
        }

        // mha1 -> mha3 : db1,db3;mha3->mha1: db1
        // mha4 -> mha1: db3
        // mha1 migrate to mha2 ["db1","db2"]
        mockNormalPreStartCopyAllDbReplication();
        migrationTaskTbl.setStatus(MigrationStatusEnum.INIT.getStatus());
        Assert.assertTrue(dbMigrationService.preStartDbMigrationTask(migrationTaskTbl.getId()));
        Mockito.verify(dbReplicationTblDao,Mockito.times(5)).insertWithReturnId(Mockito.any(DbReplicationTbl.class));
        Mockito.verify(dbReplicationFilterMappingTblDao,Mockito.times(4)).insertWithReturnId(Mockito.any(DbReplicationFilterMappingTbl.class));
    }
    
    
    
    @Test
    public void testStartDbMigrationTask() throws Exception {
        Mockito.when(migrationTaskTblDao.queryByPk(Mockito.eq(migrationTaskTbl.getId()))).thenReturn(migrationTaskTbl);
        migrationTaskTbl.setStatus(MigrationStatusEnum.PRE_STARTED.getStatus());
        mockNormalStartTask();
        mockReplicationWork();
        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.eq(mha3.getId()),Mockito.eq(mha1.getId()),Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mhaReplication3_1);
        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.eq(mha1.getId()),Mockito.eq(mha3.getId()),Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mhaReplication1_3);
        Mockito.when(messengerGroupTblDao.queryByMhaId(Mockito.eq(mha1.getId()),Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mha1MessengerGroup);
        MessengerTbl messengerTbl = MockEntityBuilder.buildMessengerTbl(1L, mha1MessengerGroup.getId());
        Mockito.when(messengerTblDao.queryByGroupId(Mockito.eq(mha1MessengerGroup.getId()))).thenReturn(Lists.newArrayList(messengerTbl));
        
        Assert.assertTrue(dbMigrationService.startDbMigrationTask(migrationTaskTbl.getId()));
        Mockito.verify(drcBuildServiceV2,Mockito.times(2)).autoConfigAppliersWithRealTimeGtid(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any());
        Mockito.verify(drcBuildServiceV2,Mockito.times(1)).autoConfigMessengersWithRealTimeGtid(Mockito.any());

        migrationTaskTbl.setStatus(MigrationStatusEnum.PRE_STARTED.getStatus());
        mockReplicationNotWork();
        Assert.assertTrue(dbMigrationService.startDbMigrationTask(migrationTaskTbl.getId()));
        Mockito.verify(drcBuildServiceV2,Mockito.times(2)).autoConfigAppliersWithRealTimeGtid(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any());
        Mockito.verify(drcBuildServiceV2,Mockito.times(1)).autoConfigMessengersWithRealTimeGtid(Mockito.any());
    }
    
    private void mockReplicationWork() throws SQLException {
        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.eq(mha3.getId()),Mockito.eq(mha1.getId()),Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mhaReplication3_1);
        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.eq(mha1.getId()),Mockito.eq(mha3.getId()),Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mhaReplication1_3);
        Mockito.when(messengerGroupTblDao.queryByMhaId(Mockito.eq(mha1.getId()),Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mha1MessengerGroup);
        MessengerTbl messengerTbl = MockEntityBuilder.buildMessengerTbl(1L, mha1MessengerGroup.getId());
        Mockito.when(messengerTblDao.queryByGroupId(Mockito.eq(mha1MessengerGroup.getId()))).thenReturn(Lists.newArrayList(messengerTbl));
    }

    private void mockReplicationNotWork() throws SQLException {
        MhaReplicationTbl mhaReplication1_3_copy = MockEntityBuilder.buildMhaReplicationTbl(1L,mha1,mha3);
        MhaReplicationTbl mhaReplication3_1_copy = MockEntityBuilder.buildMhaReplicationTbl(2L,mha3,mha1);
        mhaReplication1_3_copy.setDrcStatus(0);
        mhaReplication3_1_copy.setDrcStatus(0);
        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.eq(mha3.getId()),Mockito.eq(mha1.getId()),Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mhaReplication1_3_copy);
        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.eq(mha1.getId()),Mockito.eq(mha3.getId()),Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mhaReplication3_1_copy);
        Mockito.when(messengerGroupTblDao.queryByMhaId(Mockito.eq(mha1.getId()),Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mha1MessengerGroup);
        Mockito.when(messengerTblDao.queryByGroupId(Mockito.eq(mha1MessengerGroup.getId()))).thenReturn(Lists.newArrayList());
    }
    
    private void mockNormalStartTask() throws Exception {
        mockQueryTaskInfo();
        mockMigrateDbsReplicationInfos();
        
        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.eq(mha3.getId()),Mockito.eq(mha2.getId()),Mockito.eq(0)))
                .thenReturn(mhaReplication3_2);
        ApplierGroupTblV2 applierGroupTblV2 = new ApplierGroupTblV2();
        Mockito.when(applierGroupTblV2Dao.queryByMhaReplicationId(Mockito.eq(mhaReplication3_2.getId()),Mockito.eq(0)))
                .thenReturn(applierGroupTblV2);
        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.eq(mha2.getId()),Mockito.eq(mha3.getId()),Mockito.eq(0)))
                .thenReturn(mhaReplication2_3);
        Mockito.when(applierGroupTblV2Dao.queryByMhaReplicationId(Mockito.eq(mhaReplication2_3.getId()),Mockito.eq(0)))
                .thenReturn(applierGroupTblV2);
        Mockito.doNothing().when(drcBuildServiceV2).autoConfigAppliersWithRealTimeGtid(
               Mockito.any(MhaReplicationTbl.class),Mockito.any(ApplierGroupTblV2.class)
                , Mockito.any(MhaTblV2.class),Mockito.any(MhaTblV2.class)
        );
        Mockito.doNothing().when(drcBuildServiceV2).autoConfigMessengersWithRealTimeGtid(Mockito.any(MhaTblV2.class));
        Mockito.when(metaGeneratorV3.getDrc()).thenThrow(new Exception("mock exception"));

        Mockito.when(migrationTaskTblDao.update(Mockito.any(MigrationTaskTbl.class))).thenReturn(1);
    }
    
    private void mockQueryTaskInfo() throws Exception {
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("mha1"))).thenReturn(mha1);
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("mha2"))).thenReturn(mha2);
        List<DbTbl> migrateDbTbls = Lists.newArrayList(db1, db2);
        Mockito.when(dbTblDao.queryByDbNames(Mockito.eq(Lists.newArrayList("db1","db2")))).thenReturn(migrateDbTbls);
    }
    
    private void mockConfigNotEqual() throws Exception {
        mockQueryTaskInfo();
        Mockito.when(mysqlServiceV2.preCheckMySqlConfig(Mockito.eq("mha1"))).thenReturn(new HashMap<String,Object>() {{put("config1","value1");}});
        Mockito.when(mysqlServiceV2.preCheckMySqlConfig(Mockito.eq("mha2"))).thenReturn(new HashMap<String,Object>() {{put("config1","value2");}});
    }
    
    private void mockNormalPreStartCopyMigrateDbsReplication() throws Exception {
        mockQueryTaskInfo();
        Mockito.when(mysqlServiceV2.preCheckMySqlConfig(Mockito.eq("mha1"))).thenReturn(new HashMap<String,Object>() {{put("config1","value1");}});
        Mockito.when(mysqlServiceV2.preCheckMySqlConfig(Mockito.eq("mha2"))).thenReturn(new HashMap<String,Object>() {{put("config1","value1");}});
        mockMigrateDbsReplicationInfos();
        Mockito.doNothing().when(mhaDbMappingService).buildMhaDbMappings(Mockito.eq("mha2"),Mockito.eq(Lists.newArrayList("db1","db2")));
        Mockito.when(mhaDbMappingTblDao.queryByDbIdsAndMhaIds(Mockito.eq(Lists.newArrayList(db1.getId())),Mockito.eq(Lists.newArrayList(mha2.getId())))).thenReturn(Lists.newArrayList(mha2Db1Mapping,mha2Db2Mapping));
        //initDbReplicationTblsInNewMha
        Mockito.when(dbReplicationTblDao.queryMappingIds(Lists.newArrayList(mha3Db1Mapping.getId(),mha3Db2Mapping.getId()))).thenReturn(Lists.newArrayList());
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Lists.newArrayList(db1ReplicaInMha1_3.getId()))).thenReturn(Lists.newArrayList(rowsFilterRule1));
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Lists.newArrayList(db3ReplicaInMha1_3.getId()))).thenReturn(Lists.newArrayList());
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Lists.newArrayList(db1ReplicaInMha3_1.getId()))).thenReturn(Lists.newArrayList(columnsFilterRule2));
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Lists.newArrayList(db1MqReplicaInMha1.getId()))).thenReturn(Lists.newArrayList(mqFilterRule3));
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Lists.newArrayList(db3MqReplicaInMha1.getId()))).thenReturn(Lists.newArrayList(mqFilterRule4));
        Mockito.when(dbReplicationTblDao.insertWithReturnId(Mockito.any(DbReplicationTbl.class))).thenReturn(1L);
        Mockito.when(dbReplicationFilterMappingTblDao.insertWithReturnId(Mockito.any(DbReplicationFilterMappingTbl.class))).thenReturn(1L);
        // initMhaReplicationsAndApplierGroups
        Mockito.when(mhaReplicationTblDao.insertOrReCover(Mockito.anyLong(),Mockito.anyLong())).thenReturn(1L);
        Mockito.when(applierGroupTblV2Dao.insertOrReCover(Mockito.anyLong(),Mockito.any())).thenReturn(1L);
        // initReplicatorGroupAndMessengerGroup
        Mockito.when(replicatorGroupTblDao.upsertIfNotExist(Mockito.anyLong())).thenReturn(1L);
        Mockito.when(messengerGroupTblDao.upsertIfNotExist(Mockito.anyLong(),Mockito.anyLong(),Mockito.any())).thenReturn(1L);

        Mockito.doNothing().when(drcBuildServiceV2).autoConfigReplicatorsWithRealTimeGtid(Mockito.any(MhaTblV2.class));
        Mockito.when(metaGeneratorV3.getDrc()).thenThrow(new Exception("mock exception"));
        Mockito.when(migrationTaskTblDao.update(Mockito.any(MigrationTaskTbl.class))).thenReturn(1);
    }
    
    private void mockNormalPreStartCopyAllDbReplication() throws Exception {
        // mha1 -> mha3 : db1,db3;mha3->mha1: db1
        // mha4 -> mha1: db3
        // mha1 migrate to mha2 ["db1","db2"]
        mockQueryTaskInfo();
        Mockito.when(mysqlServiceV2.preCheckMySqlConfig(Mockito.eq("mha1"))).thenReturn(new HashMap<String,Object>() {{put("config1","value1");}});
        Mockito.when(mysqlServiceV2.preCheckMySqlConfig(Mockito.eq("mha2"))).thenReturn(new HashMap<String,Object>() {{put("config1","value1");}});
        mockMigrateDbsReplicationInfos();
        mockReplicationInfoInMha1_3();
        mockInitMhaDbMappings();
        
        //initDbReplicationTblsInNewMha
        Mockito.when(dbReplicationTblDao.queryMappingIds(Lists.newArrayList(mha3Db1Mapping.getId(),mha3Db3Mapping.getId()))).thenReturn(Lists.newArrayList());
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Lists.newArrayList(db1ReplicaInMha1_3.getId()))).thenReturn(Lists.newArrayList(rowsFilterRule1));
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Lists.newArrayList(db1ReplicaInMha3_1.getId()))).thenReturn(Lists.newArrayList(columnsFilterRule2));
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Lists.newArrayList(db1MqReplicaInMha1.getId()))).thenReturn(Lists.newArrayList(mqFilterRule3));
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Lists.newArrayList(db3MqReplicaInMha1.getId()))).thenReturn(Lists.newArrayList(mqFilterRule4));
        Mockito.when(dbReplicationTblDao.insertWithReturnId(Mockito.any(DbReplicationTbl.class))).thenReturn(1L);
        Mockito.when(dbReplicationFilterMappingTblDao.insertWithReturnId(Mockito.any(DbReplicationFilterMappingTbl.class))).thenReturn(1L);
        // initMhaReplicationsAndApplierGroups
        Mockito.when(mhaReplicationTblDao.insertOrReCover(Mockito.anyLong(),Mockito.anyLong())).thenReturn(1L);
        Mockito.when(applierGroupTblV2Dao.insertOrReCover(Mockito.anyLong(),Mockito.any())).thenReturn(1L);
        // initReplicatorGroupAndMessengerGroup
        Mockito.when(replicatorGroupTblDao.upsertIfNotExist(Mockito.anyLong())).thenReturn(1L);
        Mockito.when(messengerGroupTblDao.upsertIfNotExist(Mockito.anyLong(),Mockito.anyLong(),Mockito.any())).thenReturn(1L);

        Mockito.doNothing().when(drcBuildServiceV2).autoConfigReplicatorsWithRealTimeGtid(Mockito.any(MhaTblV2.class));
        Mockito.when(metaGeneratorV3.getDrc()).thenThrow(new Exception("mock exception"));
        Mockito.when(migrationTaskTblDao.update(Mockito.any(MigrationTaskTbl.class))).thenReturn(1);
    }
    
    
    
    private void mockReplicationInfoInMha1_3() throws SQLException {
        Mockito.when(mhaDbMappingTblDao.queryByMhaId(Mockito.eq(mha1.getId()))).thenReturn(Lists.newArrayList(mha1Db1Mapping,mha1Db2Mapping,mha1Db3Mapping));
        Mockito.when(mhaDbMappingTblDao.queryByMhaId(Mockito.eq(mha3.getId()))).thenReturn(Lists.newArrayList(mha3Db1Mapping,mha3Db3Mapping));
        Mockito.when(dbReplicationTblDao.queryByMappingIds(
                Mockito.eq(Lists.newArrayList(mha1Db1Mapping.getId(),mha1Db2Mapping.getId(),mha1Db3Mapping.getId())),
                Mockito.eq(Lists.newArrayList(mha3Db1Mapping.getId(),mha3Db3Mapping.getId())),
                Mockito.eq(DB_TO_DB.getType()))).thenReturn(Lists.newArrayList(db1ReplicaInMha1_3,db3ReplicaInMha1_3));
        Mockito.when(dbReplicationTblDao.queryByMappingIds(
                Mockito.eq(Lists.newArrayList(mha3Db1Mapping.getId(),mha3Db3Mapping.getId())),
                Mockito.eq(Lists.newArrayList(mha1Db1Mapping.getId(),mha1Db2Mapping.getId(),mha1Db3Mapping.getId())),
                Mockito.eq(DB_TO_DB.getType()))).thenReturn(Lists.newArrayList(db1ReplicaInMha3_1));
        Mockito.when(dbReplicationTblDao.queryBySrcMappingIds(
                Mockito.eq(Lists.newArrayList(mha1Db1Mapping.getId(),mha1Db2Mapping.getId(),mha1Db3Mapping.getId())),
                Mockito.eq(DB_TO_MQ.getType()))).thenReturn(Lists.newArrayList(db1MqReplicaInMha1,db3MqReplicaInMha1));
    }
    
    private void mockInitMhaDbMappings() throws SQLException {
        Mockito.doNothing().when(mhaDbMappingService).copyAndInitMhaDbMappings(Mockito.eq(mha3),Mockito.eq(Lists.newArrayList(mha1Db1Mapping,mha1Db3Mapping)));
        Mockito.when(mhaDbMappingTblDao.queryByDbIdsAndMhaIds(
                Mockito.eq(Lists.newArrayList(mha1Db1Mapping.getDbId(),mha1Db3Mapping.getDbId())),
                Mockito.eq(Lists.newArrayList(mha2.getId())))
        ).thenReturn(Lists.newArrayList(mha2Db1Mapping,mha2Db3Mapping));
    }
    
    
    // db1 mha1 migrate to  mha2
    private void mockMigrateDbsReplicationInfos() throws SQLException{
        // check and init mha info
        DbMigrationParam dbMigrationParam = mockDbMigrationParam();
        MachineTbl machineTbl1 = MockEntityBuilder.buildMachineTbl();
        mha1.setMonitorSwitch(0);
        Mockito.when(machineTblDao.queryByIpPort(Mockito.eq("ip1"), Mockito.eq(3306))).thenReturn(machineTbl1);
        Mockito.when(mhaTblV2Dao.queryByPk(Mockito.eq(1L))).thenReturn(mha1);
        Mockito.when(machineTblDao.queryByIpPort(Mockito.eq("ip2"), Mockito.eq(3306))).thenReturn(null);
        Mockito.when(drcBuildServiceV2.syncMhaInfoFormDbaApi(Mockito.eq("mha2"))).thenReturn(mha2);
        Mockito.when(mhaDbReplicationService.isDbReplicationExist(Mockito.anyLong(),Mockito.anyList())).thenReturn(true);
        // getReplicationInfoInOldMha
        List<DbTbl> dbTbls = Lists.newArrayList(db1, db2);
        Mockito.when(dbTblDao.queryByDbNames(Mockito.eq(dbMigrationParam.getDbs()))).thenReturn(dbTbls);
        // db1: mha1<->mha3 & mqReplication
        Mockito.when(mhaDbMappingTblDao.queryByDbIdAndMhaId(Mockito.eq(db1.getId()),Mockito.eq(mha1.getId()))).thenReturn(mha1Db1Mapping);
        Mockito.when(dbReplicationTblDao.queryBySrcMappingIds(Mockito.eq(Lists.newArrayList(mha1Db1Mapping.getId())),Mockito.eq(DB_TO_DB.getType())))
                .thenReturn(Lists.newArrayList(db1ReplicaInMha1_3));
        Mockito.when(dbReplicationTblDao.queryByDestMappingIds(Mockito.eq(Lists.newArrayList(mha1Db1Mapping.getId())),Mockito.eq(DB_TO_DB.getType())))
                .thenReturn(Lists.newArrayList(db1ReplicaInMha3_1));
        Mockito.when(dbReplicationTblDao.queryBySrcMappingIds(Mockito.eq(Lists.newArrayList(mha1Db1Mapping.getId())),Mockito.eq(DB_TO_MQ.getType())))
                .thenReturn(Lists.newArrayList(db1MqReplicaInMha1));
        // getAnotherMhaTblsInDest
        Mockito.when(mhaDbMappingTblDao.queryByIds(Mockito.eq(Lists.newArrayList(db1ReplicaInMha1_3.getDstMhaDbMappingId())))).thenReturn(Lists.newArrayList(mha3Db1Mapping));
        // getAnotherMhaTblsInSrc
        Mockito.when(mhaDbMappingTblDao.queryByIds(Mockito.eq(Lists.newArrayList(db1ReplicaInMha3_1.getSrcMhaDbMappingId())))).thenReturn(Lists.newArrayList(mha3Db1Mapping));
        // getMhaTbls
        Mockito.when(mhaTblV2Dao.queryByIds(Mockito.eq(Lists.newArrayList(mha3Db1Mapping.getId())))).thenReturn(Lists.newArrayList(mha3));
        Mockito.when(mhaTblV2Dao.queryByIds(Mockito.eq(Lists.newArrayList()))).thenReturn(Lists.newArrayList());
        
        // db2: no mhaReplication
        Mockito.when(mhaDbMappingTblDao.queryByDbIdAndMhaId(Mockito.eq(db2.getId()),Mockito.eq(mha1.getId()))).thenReturn(null);
        
        DcDo dcDo1 = new DcDo();
        dcDo1.setDcId(1L);
        dcDo1.setDcName("dc1");
        dcDo1.setRegionName("region1");
        DcDo dcDo2 = new DcDo();
        dcDo2.setDcId(2L);
        dcDo2.setDcName("dc2");
        dcDo2.setRegionName("region2");
        Mockito.when(metaInfoServiceV2.queryAllDcWithCache()).thenReturn(Lists.newArrayList(dcDo1, dcDo2));
        
        Mockito.when(mhaReplicationTblDao.queryByRelatedMhaId(Mockito.eq(Lists.newArrayList(mha1.getId())))).thenReturn(Lists.newArrayList(mhaReplication1_3,mhaReplication3_1));
        Mockito.when(mhaReplicationTblDao.queryByRelatedMhaId(Mockito.eq(Lists.newArrayList(mha2.getId())))).thenReturn(Lists.newArrayList());
        Mockito.when(migrationTaskTblDao.insertWithReturnId(Mockito.any())).thenReturn(1L);
        
    }
    
    private void mockDbMigrationCheckForbiddenCase1() throws SQLException{
        // db2: mha4->mha1
        Mockito.when(mhaDbMappingTblDao.queryByDbIdAndMhaId(Mockito.eq(db2.getId()),Mockito.eq(mha1.getId()))).thenReturn(mha1Db2Mapping);
        Mockito.when(dbReplicationTblDao.queryBySrcMappingIds(Mockito.eq(Lists.newArrayList(mha1Db2Mapping.getId())),Mockito.eq(DB_TO_DB.getType())))
                .thenReturn(Lists.newArrayList());
        Mockito.when(dbReplicationTblDao.queryByDestMappingIds(Mockito.eq(Lists.newArrayList(mha1Db2Mapping.getId())),Mockito.eq(DB_TO_DB.getType())))
                .thenReturn(Lists.newArrayList(db2ReplicaInMha4_1));
        Mockito.when(dbReplicationTblDao.queryBySrcMappingIds(Mockito.eq(Lists.newArrayList(mha1Db2Mapping.getId())),Mockito.eq(DB_TO_MQ.getType())))
                .thenReturn(Lists.newArrayList());
        // getAnotherMhaTblsInDest
        Mockito.when(mhaDbMappingTblDao.queryByIds(Mockito.eq(Lists.newArrayList(db2ReplicaInMha4_1.getSrcMhaDbMappingId())))).thenReturn(Lists.newArrayList(mha4Db2Mapping));
        Mockito.when(mhaTblV2Dao.queryByIds(Mockito.eq(Lists.newArrayList(mha4Db2Mapping.getMhaId())))).thenReturn(Lists.newArrayList(mha4));
        // getAnotherMhaTblsInSrc
        Mockito.when(mhaDbMappingTblDao.queryByIds(Mockito.eq(Lists.newArrayList()))).thenReturn(Lists.newArrayList());
        Mockito.when(mhaTblV2Dao.queryByIds(Mockito.eq(Lists.newArrayList()))).thenReturn(Lists.newArrayList());
    }

    private void mockDbMigrationCheckForbiddenCase2() throws SQLException{
        Mockito.when(mhaReplicationTblDao.queryByRelatedMhaId(Mockito.eq(Lists.newArrayList(mha1.getId())))).thenReturn(Lists.newArrayList(mhaReplication1_3,mhaReplication3_1));
        Mockito.when(mhaReplicationTblDao.queryByRelatedMhaId(Mockito.eq(Lists.newArrayList(mha2.getId())))).thenReturn(Lists.newArrayList(mhaReplication2_3));
        Mockito.when(mhaTblV2Dao.queryByPk(Mockito.eq(Lists.newArrayList(mha3.getId())))).thenReturn(Lists.newArrayList(mha3));
    }
    
    private DbMigrationParam mockDbMigrationParam() {
        // new DbMigrationParam set all fields
        DbMigrationParam dbMigrationParam = new DbMigrationParam();
        dbMigrationParam.setOperator("operator");
        dbMigrationParam.setDbs(Lists.newArrayList("db1", "db2"));
        
        MigrateMhaInfo migrateMhaInfo = new MigrateMhaInfo();
        migrateMhaInfo.setName("mha1");
        migrateMhaInfo.setMasterIp("ip1");
        migrateMhaInfo.setMasterPort(3306);
        dbMigrationParam.setOldMha(migrateMhaInfo);
        
        MigrateMhaInfo migrateMhaInfo1 = new MigrateMhaInfo();
        migrateMhaInfo1.setName("mha2");
        migrateMhaInfo1.setMasterIp("ip2");
        migrateMhaInfo1.setMasterPort(3306);
        dbMigrationParam.setNewMha(migrateMhaInfo1);
        
        return dbMigrationParam; 
    }
    
}