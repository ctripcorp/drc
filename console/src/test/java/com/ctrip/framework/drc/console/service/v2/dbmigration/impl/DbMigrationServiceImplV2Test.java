package com.ctrip.framework.drc.console.service.v2.dbmigration.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierGroupTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.MhaDbReplicationTbl;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.dao.v3.ApplierGroupTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.ApplierTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.MhaDbReplicationTblDao;
import com.ctrip.framework.drc.console.dto.v2.*;
import com.ctrip.framework.drc.console.dto.v3.MhaDbDto;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.MigrationStatusEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.console.enums.ReplicationTypeEnum.DB_TO_DB;
import static com.ctrip.framework.drc.console.enums.ReplicationTypeEnum.DB_TO_MQ;

/**
 * Created by shiruixin
 * 2024/9/24 17:25
 */
public class DbMigrationServiceImplV2Test {

    @InjectMocks
    private DbMigrationServiceImplV2 dbMigrationService;
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
    private MhaDbMappingService mhaDbMappingService;
    @Mock
    private DrcBuildServiceV2 drcBuildServiceV2;
    @Mock
    private DbDrcBuildService dbDrcBuildService;
    @Mock
    private MysqlServiceV2 mysqlServiceV2;
    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private MhaDbReplicationService mhaDbReplicationService;
    @Mock
    private MhaReplicationServiceV2 mhaReplicationServiceV2;
    @Mock
    private CacheMetaService cacheMetaService;
    @Mock
    private MhaDbReplicationTblDao mhaDbReplicationTblDao;
    @Mock
    private ApplierGroupTblV3Dao applierGroupTblV3Dao;
    @Mock
    private ApplierTblV3Dao applierTblV3Dao;
    @Mock
    private MessengerServiceV2 messengerServiceV2;


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
    MessengerGroupTbl mha2MessengerGroup = MockEntityBuilder.buildMessengerGroupTbl(2L, mha2.getId());

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
    DbReplicationTbl db1MqReplicaInMha2 = MockEntityBuilder.buildDbReplicationTbl(8L, mha2Db1Mapping, null,1);
    DbReplicationTbl db1ReplicaInMha2_3 = MockEntityBuilder.buildDbReplicationTbl(9L, mha2Db1Mapping, mha3Db1Mapping,0);
    DbReplicationTbl db1ReplicaInMha3_2 = MockEntityBuilder.buildDbReplicationTbl(10L, mha3Db1Mapping, mha2Db1Mapping, 0);


    DbReplicationFilterMappingTbl rowsFilterRule1 = MockEntityBuilder.buildDbReplicationFilterMappingTbl(1L,db1ReplicaInMha1_3,1L,null,null);
    DbReplicationFilterMappingTbl columnsFilterRule2 = MockEntityBuilder.buildDbReplicationFilterMappingTbl(2L,db1ReplicaInMha3_1,null,1L,null);
    DbReplicationFilterMappingTbl mqFilterRule3 = MockEntityBuilder.buildDbReplicationFilterMappingTbl(3L,db1MqReplicaInMha1,null,null,1L);
    DbReplicationFilterMappingTbl mqFilterRule4 = MockEntityBuilder.buildDbReplicationFilterMappingTbl(3L,db3MqReplicaInMha1,null,null,1L);

    MhaDbReplicationTbl mha1db1_Mha3db1 = MockEntityBuilder.buildMhaDbReplicationTbl(1L, mha1Db1Mapping, mha3Db1Mapping, DB_TO_DB.getType());
    MhaDbReplicationTbl mha2db1_Mha3db1 = MockEntityBuilder.buildMhaDbReplicationTbl(2L, mha2Db1Mapping, mha3Db1Mapping,  DB_TO_DB.getType());
    MhaDbReplicationTbl mha3db1_Mha1db1 = MockEntityBuilder.buildMhaDbReplicationTbl(3L, mha3Db1Mapping, mha1Db1Mapping, DB_TO_DB.getType());
    MhaDbReplicationTbl mha3db1_Mha2db1 = MockEntityBuilder.buildMhaDbReplicationTbl(4L, mha3Db1Mapping, mha2Db1Mapping,  DB_TO_DB.getType());

    ApplierGroupTblV3 aGroupV3_mha2db1_Mha3db1 = MockEntityBuilder.buildDbApplierGroup(1L, mha2db1_Mha3db1.getId());
    ApplierGroupTblV3 aGroupV3_mha3db1_Mha2db1 = MockEntityBuilder.buildDbApplierGroup(2L, mha3db1_Mha2db1.getId());
    ApplierGroupTblV3 aGroupV3_mha3db1_Mha1db1 = MockEntityBuilder.buildDbApplierGroup(3L, mha3db1_Mha1db1.getId());
    ApplierGroupTblV3 aGroupV3_mha1db1_Mha3db1 = MockEntityBuilder.buildDbApplierGroup(4L, mha1db1_Mha3db1.getId());

    MigrationTaskTbl migrationTaskTbl = MockEntityBuilder.buildMigrationTaskTbl(1L,"mha1","mha2","[\"db1\",\"db2\"]","drctest");

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        Mockito.when(migrationTaskTblDao.update(Mockito.any(MigrationTaskTbl.class))).thenReturn(1);
        Mockito.when(mhaTblV2Dao.update(Mockito.any(MhaTblV2.class))).thenReturn(1);
        Mockito.when(cacheMetaService.refreshMetaCache()).thenReturn(true);
    }

    @Test
    public void testDbMigrationCheckAndCreateTaskInDbGranularity() throws Exception {
        Mockito.when(consoleConfig.getDbMigrationSwitch()).thenReturn(true);
        DbMigrationParam dbMigrationParam = mockDbMigrationParam();
        try {
            mockNormalPreStartCopyAllDbReplication();
            Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.anyLong(), Mockito.anyLong(), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(PojoBuilder.getMhaReplicationTbl());
            Mockito.when(applierGroupTblV2Dao.queryByMhaReplicationId(Mockito.anyLong(), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(PojoBuilder.getApplierGroupTblV2s().get(0));
            Mockito.when(applierTblV2Dao.queryByApplierGroupId(Mockito.anyLong(), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(Lists.newArrayList());
            dbMigrationService.dbMigrationCheckAndCreateTask(dbMigrationParam);
        } catch (ConsoleException e) {
            Assert.assertEquals("mha1->mha2 replication is in mha mode, please contact DRC team!",e.getMessage());
        }



    }

    @Test
    public void testDbMigrationCheckAndCreateTask() throws SQLException {
        Mockito.when(consoleConfig.getDbMigrationSwitch()).thenReturn(false);
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
        Mockito.when(mhaReplicationServiceV2.queryAllHasActiveMhaDbReplications()).thenReturn(Lists.newArrayList());
        mockMigrateDbsReplicationInfos();
        Pair<String, Long> stringLongPair = dbMigrationService.dbMigrationCheckAndCreateTask(dbMigrationParam);
        Assert.assertEquals(1L,stringLongPair.getRight().longValue());
        Mockito.verify(drcBuildServiceV2,Mockito.times(1)).syncMhaInfoFormDbaApi(Mockito.eq("mha2"), Mockito.any());
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

        // check case has dbmode replication TODO
//        try {
//            mockMigrateDbsReplicationInfos();
//            mockDbMigrationCheckForbiddenCase2();
//            Mockito.when(mhaReplicationServiceV2.queryAllHasActiveMhaDbReplications()).thenReturn(getReplications());
//            dbMigrationService.dbMigrationCheckAndCreateTask(dbMigrationParam);
//        } catch (ConsoleException e) {
//            Assert.assertTrue(e.getMessage().contains("Mha has db mode replication, please contact DRC team!"));
//        }


    }

    private List<MhaReplicationTbl> getReplications() {
        MhaReplicationTbl mhaReplicationTbl1 = MockEntityBuilder.buildMhaReplicationTbl(1L, mha1, mha3);
        return Lists.newArrayList(mhaReplicationTbl1);
    }

    @Test
    public void testCancelMigrationTaskInDbGranularity() throws Exception {
        Mockito.when(consoleConfig.getDbMigrationSwitch()).thenReturn(true);
        Mockito.when(migrationTaskTblDao.queryById(Mockito.eq(migrationTaskTbl.getId()))).thenReturn(migrationTaskTbl);
        migrationTaskTbl.setStatus(MigrationStatusEnum.PRE_STARTED.getStatus());
        mockNormalPreStartCopyAllDbReplication();
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq(mha1.getMhaName()), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mha1);
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq(mha2.getMhaName()), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mha2);
        Mockito.when(mhaDbMappingTblDao.queryByDbIds(Mockito.anyList())).thenReturn(Lists.newArrayList(mha1Db1Mapping,mha2Db1Mapping,mha3Db1Mapping,mha1Db2Mapping));
        Mockito.when(mhaDbMappingTblDao.queryByMhaId(Mockito.eq(mha2.getId()))).thenReturn(Lists.newArrayList(mha2Db1Mapping));
        Mockito.when(dbReplicationTblDao.queryBySrcMappingIds(Mockito.anyList(),Mockito.eq(DB_TO_MQ.getType())))
                .thenReturn(Lists.newArrayList(db1MqReplicaInMha2))
                .thenReturn(Lists.newArrayList());
        Mockito.when(dbReplicationTblDao.queryByMappingIds(Mockito.anyList(), Mockito.anyList() ,Mockito.eq(DB_TO_DB.getType())))
                .thenReturn(Lists.newArrayList(db1ReplicaInMha2_3))
                .thenReturn(Lists.newArrayList(db1ReplicaInMha3_2))
                .thenReturn(Lists.newArrayList());

        Mockito.when(messengerGroupTblDao.queryByMhaId(Mockito.eq(mha2.getId()), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mha2MessengerGroup);
        Mockito.when(messengerTblDao.queryByGroupId(Mockito.eq(mha2MessengerGroup.getMhaId()))).thenReturn(Lists.newArrayList(new MessengerTbl()));
        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.eq(mha2.getId()), Mockito.eq(mha3.getId()), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mhaReplication2_3);
        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.eq(mha3.getId()), Mockito.eq(mha2.getId()), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mhaReplication3_2);

        Assert.assertTrue(dbMigrationService.cancelTask(1L));

        Mockito.verify(mhaDbReplicationService, Mockito.times(1)).offlineMhaDbReplication(Mockito.anyList());
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.times(0)).update(Mockito.any(DbReplicationFilterMappingTbl.class));
        Mockito.verify(messengerGroupTblDao, Mockito.times(1)).update(Mockito.any(MessengerGroupTbl.class));
        Mockito.verify(mhaDbReplicationService, Mockito.times(2)).offlineMhaDbReplicationAndApplierV3(Mockito.anyList());
        Mockito.verify(mhaReplicationTblDao, Mockito.times(2)).update(Mockito.any(MhaReplicationTbl.class));
        Mockito.verify(mhaDbMappingTblDao, Mockito.times(1)).delete(Mockito.anyList());
    }

    @Test
    public void testOfflineOldDrcConfigInDbGranularity() throws Exception {
        Mockito.when(consoleConfig.getDbMigrationSwitch()).thenReturn(true);
        Mockito.when(migrationTaskTblDao.queryById(Mockito.eq(migrationTaskTbl.getId()))).thenReturn(migrationTaskTbl);
        migrationTaskTbl.setStatus(MigrationStatusEnum.READY_TO_COMMIT_TASK.getStatus());
        mockNormalPreStartCopyAllDbReplication();
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq(mha1.getMhaName()), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mha1);
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq(mha2.getMhaName()), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mha2);
        Mockito.when(mhaDbMappingTblDao.queryByDbIds(Mockito.anyList())).thenReturn(Lists.newArrayList(mha1Db1Mapping,mha2Db1Mapping,mha3Db1Mapping,mha1Db2Mapping));
        Mockito.when(mhaDbMappingTblDao.queryByMhaId(Mockito.eq(mha2.getId()))).thenReturn(Lists.newArrayList(mha2Db1Mapping));
        Mockito.when(dbReplicationTblDao.queryBySrcMappingIds(Mockito.anyList(),Mockito.eq(DB_TO_MQ.getType())))
                .thenReturn(Lists.newArrayList(db1MqReplicaInMha2))
                .thenReturn(Lists.newArrayList());
        Mockito.when(dbReplicationTblDao.queryByMappingIds(Mockito.anyList(), Mockito.anyList() ,Mockito.eq(DB_TO_DB.getType())))
                .thenReturn(Lists.newArrayList(db1ReplicaInMha2_3))
                .thenReturn(Lists.newArrayList(db1ReplicaInMha3_2))
                .thenReturn(Lists.newArrayList());

        Mockito.when(messengerGroupTblDao.queryByMhaId(Mockito.eq(mha2.getId()), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mha2MessengerGroup);
        Mockito.when(messengerTblDao.queryByGroupId(Mockito.eq(mha2MessengerGroup.getMhaId()))).thenReturn(Lists.newArrayList(new MessengerTbl()));
        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.eq(mha2.getId()), Mockito.eq(mha3.getId()), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mhaReplication2_3);
        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.eq(mha3.getId()), Mockito.eq(mha2.getId()), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mhaReplication3_2);

        dbMigrationService.offlineOldDrcConfig(1L);
        Mockito.verify(mhaDbReplicationService, Mockito.times(2)).offlineMhaDbReplicationAndApplierV3(Mockito.anyList());
        Mockito.verify(mhaReplicationTblDao, Mockito.times(2)).update(Mockito.any(MhaReplicationTbl.class));
        Mockito.verify(mhaDbMappingTblDao, Mockito.times(1)).delete(Mockito.anyList());
    }

    @Test
    public void testPreStartDbMigrationTaskInDbGranularity() throws Exception {
        Mockito.when(consoleConfig.getDbMigrationSwitch()).thenReturn(true);
        Mockito.when(migrationTaskTblDao.queryByPk(Mockito.eq(migrationTaskTbl.getId()))).thenReturn(migrationTaskTbl);
        Mockito.when(consoleConfig.getConfgiCheckSwitch()).thenReturn(true);

        mockNormalPreStartCopyAllDbReplication();

        Mockito.when(mhaDbMappingTblDao.queryByDbIdsAndMhaIds(Mockito.eq(Lists.newArrayList(db1.getId())), Mockito.eq(Lists.newArrayList(mha2.getId())))).thenReturn(Lists.newArrayList(mha2Db1Mapping));
        Mockito.when(mhaDbReplicationTblDao.queryBySamples(Mockito.anyList()))
                .thenReturn(Lists.newArrayList())
                .thenReturn(Lists.newArrayList(mha2db1_Mha3db1, mha3db1_Mha2db1));
        Mockito.when(mhaDbReplicationTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[]{1,1});
        Mockito.when(applierGroupTblV3Dao.insertOrReCover(Mockito.eq(mha2db1_Mha3db1.getId()), Mockito.isNull())).thenReturn(aGroupV3_mha2db1_Mha3db1.getId());
        Mockito.when(applierGroupTblV3Dao.insertOrReCover(Mockito.eq(mha3db1_Mha2db1.getId()), Mockito.isNull())).thenReturn(aGroupV3_mha3db1_Mha2db1.getId());
        Mockito.when(mysqlServiceV2.createDrcMonitorDbTable(Mockito.any())).thenReturn(true);

        migrationTaskTbl.setStatus(MigrationStatusEnum.INIT.getStatus());
        Assert.assertTrue(dbMigrationService.preStartDbMigrationTask(migrationTaskTbl.getId()));
        Mockito.verify(mhaDbMappingService, Mockito.times(1)).copyAndInitMhaDbMappings(Mockito.any(MhaTblV2.class), Mockito.anyList());
        Mockito.verify(mhaDbReplicationTblDao, Mockito.times(1)).batchInsert(Mockito.anyList());
        Mockito.verify(applierGroupTblV3Dao, Mockito.times(2)).insertOrReCover(Mockito.anyLong(), Mockito.isNull());
        Mockito.verify(dbReplicationTblDao,Mockito.times(3)).insertWithReturnId(Mockito.any(DbReplicationTbl.class));
        Mockito.verify(mhaReplicationTblDao, Mockito.times(2)).insertOrReCover(Mockito.anyLong(), Mockito.anyLong());
        Mockito.verify(replicatorGroupTblDao, Mockito.times(1)).upsertIfNotExist(Mockito.anyLong());
        Mockito.verify(messengerGroupTblDao, Mockito.times(1)).upsertIfNotExist(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString());
    }

    @Test
    public void testPreStartDbMigrationTask() throws Exception {
        Mockito.when(consoleConfig.getDbMigrationSwitch()).thenReturn(false);
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
        Mockito.when(mysqlServiceV2.createDrcMonitorDbTable(Mockito.any())).thenReturn(false);
        try {
            Assert.assertTrue(dbMigrationService.preStartDbMigrationTask(migrationTaskTbl.getId()));
        } catch (ConsoleException e) {
            Assert.assertTrue(e.getMessage().contains("Can not create DRC Db Monitor Table"));
        }

        Mockito.when(mysqlServiceV2.createDrcMonitorDbTable(Mockito.any())).thenReturn(true);
    }

    @Test
    public void testStartDbMigrationTaskInDbGranularity() throws Exception {
        Mockito.when(consoleConfig.getDbMigrationSwitch()).thenReturn(true);
        Mockito.when(migrationTaskTblDao.queryByPk(Mockito.eq(migrationTaskTbl.getId()))).thenReturn(migrationTaskTbl);
        migrationTaskTbl.setStatus(MigrationStatusEnum.PRE_STARTED.getStatus());
        mockNormalPreStartCopyAllDbReplication();
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq(mha1.getMhaName()), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mha1);
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq(mha2.getMhaName()), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mha2);
        Mockito.when(mhaDbMappingTblDao.queryByMhaIdAndDbIds(Mockito.eq(mha2.getId()), Mockito.anyList(),Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(Lists.newArrayList(mha2Db1Mapping));
        Mockito.when(mhaDbReplicationTblDao.queryAllExist()).thenReturn(Lists.newArrayList(mha3db1_Mha2db1,mha2db1_Mha3db1,mha1db1_Mha3db1,mha3db1_Mha1db1));
        Mockito.when(applierGroupTblV3Dao.queryByMhaDbReplicationId(Mockito.eq(mha3db1_Mha2db1.getId()), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(aGroupV3_mha3db1_Mha2db1);
        Mockito.when(applierGroupTblV3Dao.queryByMhaDbReplicationId(Mockito.eq(mha3db1_Mha1db1.getId()), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(aGroupV3_mha3db1_Mha1db1);
        Mockito.when(applierTblV3Dao.queryByApplierGroupId(Mockito.eq(aGroupV3_mha3db1_Mha1db1.getId()), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(Lists.newArrayList(new ApplierTblV3()));
        Mockito.when(mhaDbMappingTblDao.queryById(Mockito.eq(mha3Db1Mapping.getId()))).thenReturn(mha3Db1Mapping);
        Mockito.when(mhaTblV2Dao.queryById(Mockito.eq(mha3Db1Mapping.getMhaId()))).thenReturn(mha3);
        Mockito.when(applierGroupTblV3Dao.queryByMhaDbReplicationId(Mockito.eq(mha2db1_Mha3db1.getId()), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(aGroupV3_mha2db1_Mha3db1);
        Mockito.when(applierGroupTblV3Dao.queryByMhaDbReplicationId(Mockito.eq(mha1db1_Mha3db1.getId()), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(aGroupV3_mha1db1_Mha3db1);
        Mockito.when(applierTblV3Dao.queryByApplierGroupId(Mockito.eq(aGroupV3_mha1db1_Mha3db1.getId()), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(Lists.newArrayList(new ApplierTblV3()));
        Mockito.when(mhaDbMappingTblDao.queryById(Mockito.eq(mha2Db1Mapping.getId()))).thenReturn(mha2Db1Mapping);
        Mockito.when(mhaTblV2Dao.queryById(Mockito.eq(mha2Db1Mapping.getMhaId()))).thenReturn(mha2);
        Mockito.when(messengerGroupTblDao.queryByMhaId(Mockito.eq(mha1.getId()), Mockito.eq(BooleanEnum.FALSE.getCode()))).thenReturn(mha1MessengerGroup);
        Mockito.when(messengerTblDao.queryByGroupId(mha1MessengerGroup.getId())).thenReturn(Lists.newArrayList(new MessengerTbl()));

        Assert.assertTrue(dbMigrationService.startDbMigrationTask(migrationTaskTbl.getId()));

        Mockito.verify(dbDrcBuildService, Mockito.times(1)).autoConfigDbAppliers(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(), Mockito.any(),Mockito.anyBoolean());
        Mockito.verify(dbDrcBuildService, Mockito.times(1)).autoConfigDbAppliersWithRealTimeGtid(Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any(),Mockito.any());
        Mockito.verify(drcBuildServiceV2, Mockito.times(1)).autoConfigMessengersWithRealTimeGtid(Mockito.any(),Mockito.anyBoolean());
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
        Mockito.when(drcBuildServiceV2.syncMhaInfoFormDbaApi(Mockito.eq("mha2"), Mockito.any())).thenReturn(mha2);
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

        DbMigrationParam.MigrateMhaInfo migrateMhaInfo = new DbMigrationParam.MigrateMhaInfo();
        migrateMhaInfo.setName("mha1");
        migrateMhaInfo.setMasterIp("ip1");
        migrateMhaInfo.setMasterPort(3306);
        dbMigrationParam.setOldMha(migrateMhaInfo);

        DbMigrationParam.MigrateMhaInfo migrateMhaInfo1 = new DbMigrationParam.MigrateMhaInfo();
        migrateMhaInfo1.setName("mha2");
        migrateMhaInfo1.setMasterIp("ip2");
        migrateMhaInfo1.setMasterPort(3306);
        dbMigrationParam.setNewMha(migrateMhaInfo1);

        return dbMigrationParam;
    }

    @Test
    public void testGetMhaDbReplicationDelayFromMigrateTask() throws Exception {
        Mockito.when(migrationTaskTblDao.queryById(Mockito.eq(1L))).thenReturn(migrationTaskTbl);
        mockNormalPreStartCopyAllDbReplication();
        MhaDbReplicationDto dto = new MhaDbReplicationDto();
        MhaDbReplicationDto dto2 = new MhaDbReplicationDto();
        MhaDbDto oldMha = new MhaDbDto(1L, "oldmha","db");
        MhaDbDto newMha = new MhaDbDto(2L, "newMha","db");
        MhaDbDto dstMha = new MhaDbDto(3L, "dstMha","db");
        dto.setSrc(oldMha);
        dto.setDst(dstMha);
        dto.setDrcStatus(Boolean.FALSE);
        dto2.setSrc(newMha);
        dto2.setDst(dstMha);
        dto2.setDrcStatus(Boolean.TRUE);
        List<MhaDbReplicationDto> mhaDbReplicationDtosAll = Lists.newArrayList(dto,dto2);
        Mockito.when(mhaDbReplicationService.queryByDbNamesAndMhaNames(Mockito.anyList(),Mockito.anyList(),Mockito.eq(DB_TO_DB))).thenReturn(mhaDbReplicationDtosAll);

        MhaDbDelayInfoDto mhaDbDelayInfoDto = new MhaDbDelayInfoDto();
        mhaDbDelayInfoDto.setDbName("db");
        mhaDbDelayInfoDto.setSrcMha("newMha");
        mhaDbDelayInfoDto.setDstMha("dstMha");
        Mockito.when(mhaDbReplicationService.getReplicationDelays(Mockito.anyList())).thenReturn(Lists.newArrayList(mhaDbDelayInfoDto));

        List<MhaApplierDto> mhaApplierDtos = dbMigrationService.getMhaDbReplicationDelayFromMigrateTask(1L);
        Assert.assertEquals(mhaApplierDtos.size(),2);
    }


    @Test
    public void testCleanApplierDirtyData() throws Exception{
        Mockito.when(mhaDbReplicationTblDao.queryAll()).thenReturn(PojoBuilder.getMhaDbReplicationTbls());
        Mockito.when(applierGroupTblV3Dao.queryByMhaDbReplicationIds(Mockito.anyList())).thenReturn(PojoBuilder.getApplierGroupTblV3s());
        Mockito.when(applierTblV3Dao.queryByApplierGroupIds(Mockito.anyList(), Mockito.eq(0))).thenReturn(PojoBuilder.getApplierTblV3s());

        Map<String, List<Long>> result = dbMigrationService.cleanApplierDirtyData(false);
        Assert.assertEquals(2,result.size());
    }
}
