package com.ctrip.framework.drc.console.service.v2;

import com.alibaba.fastjson.JSON;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.dao.v3.ApplierGroupTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.ApplierTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.MhaDbReplicationTblDao;
import com.ctrip.framework.drc.console.dto.v2.MhaDelayInfoDto;
import com.ctrip.framework.drc.console.dto.v2.MhaReplicationDto;
import com.ctrip.framework.drc.console.enums.MigrationStatusEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.service.v2.dbmigration.impl.DbMigrationServiceImpl;
import com.ctrip.framework.drc.console.service.v2.impl.MetaGeneratorV3;
import com.ctrip.framework.drc.console.service.v2.impl.MhaReplicationServiceV2Impl;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.ctrip.framework.drc.core.config.RegionConfig;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.*;

/**
 * Created by dengquanliang
 * 2023/8/23 15:03
 */
public class DbMigrationServiceTest {

    @InjectMocks
    private DbMigrationServiceImpl dbMigrateService;
    @Mock
    private MhaTblV2Dao mhaTblV2Dao;
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
    private DbReplicationFilterMappingTblDao dBReplicationFilterMappingTblDao;
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
    private MigrationTaskTblDao migrationTaskTblDao;
    @Mock
    private RegionConfig regionConfig;
    @Mock
    private MhaReplicationServiceV2Impl mhaReplicationServiceV2;
    @Mock
    private MessengerServiceV2 messengerServiceV2;
    @Mock
    private ResourceTblDao resourceTblDao;
    @Mock
    private ResourceService resourceService;
    @Mock
    private MhaServiceV2 mhaServiceV2;
    @Mock
    private DrcBuildServiceV2 drcBuildServiceV2;
    @Mock
    private MysqlServiceV2 mysqlServiceV2;
    @Mock
    private MhaDbReplicationTblDao mhaDbReplicationTblDao;
    @Mock
    private MhaDbReplicationService mhaDbReplicationService;
    @Mock
    private ApplierGroupTblV3Dao dbApplierGroupTblDao;
    @Mock
    private ApplierTblV3Dao dbApplierTblDao;
    @Mock
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Mock
    private ReplicatorTblDao replicatorTblDao;
    @Mock
    private DefaultConsoleConfig consoleConfig;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testOfflineOldDrcConfig() throws Exception {
        try (MockedStatic<HttpUtils> mockedStatic = Mockito.mockStatic(HttpUtils.class)) {
            mockedStatic.when(() -> {
                HttpUtils.post(Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.any(Map.class));
            }).thenReturn(ApiResult.getSuccessInstance(null));
            initMock();
            dbMigrateService.offlineOldDrcConfig(1L);

            Mockito.verify(dbReplicationTblDao, Mockito.times(2)).update(Mockito.anyList());
            Mockito.verify(dBReplicationFilterMappingTblDao, Mockito.times(2)).update(Mockito.anyList());
            Mockito.verify(messengerGroupTblDao, Mockito.times(1)).update(Mockito.any(MessengerGroupTbl.class));
            Mockito.verify(messengerTblDao, Mockito.times(1)).update(Mockito.anyList());
            Mockito.verify(mhaReplicationTblDao, Mockito.times(2)).update(Mockito.any(MhaReplicationTbl.class));
            Mockito.verify(applierGroupTblV2Dao, Mockito.times(2)).update(Mockito.any(ApplierGroupTblV2.class));
            Mockito.verify(applierTblV2Dao, Mockito.times(2)).update(Mockito.anyList());
            Mockito.verify(mhaDbReplicationService, Mockito.times(2)).offlineMhaDbReplication(Mockito.anyList());
        }

    }

    @Test
    public void testRollBackNewDrcConfig() throws Exception {
        try (MockedStatic<HttpUtils> mockedStatic = Mockito.mockStatic(HttpUtils.class)) {
            mockedStatic.when(() -> {
                HttpUtils.post(Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.any(Map.class));
            }).thenReturn(ApiResult.getSuccessInstance(null));
            initMock();
            dbMigrateService.rollBackNewDrcConfig(1L);


            Mockito.verify(dbReplicationTblDao, Mockito.times(2)).update(Mockito.anyList());
            Mockito.verify(dBReplicationFilterMappingTblDao, Mockito.times(2)).update(Mockito.anyList());
            Mockito.verify(messengerGroupTblDao, Mockito.times(1)).update(Mockito.any(MessengerGroupTbl.class));
            Mockito.verify(messengerTblDao, Mockito.times(1)).update(Mockito.anyList());
            Mockito.verify(mhaReplicationTblDao, Mockito.never()).update(Mockito.any(MhaReplicationTbl.class));
            Mockito.verify(applierGroupTblV2Dao, Mockito.never()).update(Mockito.any(ApplierGroupTblV2.class));
            Mockito.verify(applierTblV2Dao, Mockito.never()).update(Mockito.anyList());
            Mockito.verify(mhaDbReplicationService, Mockito.times(2)).offlineMhaDbReplication(Mockito.anyList());

        }

    }
    
    @Test
    public void testCancelMigrationTask() throws Exception {
        try (MockedStatic<HttpUtils> mockedStatic = Mockito.mockStatic(HttpUtils.class)) {
            mockedStatic.when(() -> {
                HttpUtils.post(Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.any(Map.class));
            }).thenReturn(ApiResult.getSuccessInstance(null));
            initMock();
            Mockito.when(mhaServiceV2.offlineMha(Mockito.anyString())).thenReturn(true);
            
            // case1
            MigrationTaskTbl migrationTaskTbl = getMigrationTaskTbl();
            migrationTaskTbl.setStatus(MigrationStatusEnum.STARTING.getStatus());
            Mockito.when(migrationTaskTblDao.queryById(Mockito.anyLong())).thenReturn(migrationTaskTbl);
            try {
                dbMigrateService.cancelTask(1L);
            } catch (ConsoleException e) {
                Assert.assertTrue(e.getMessage().contains("Not support cancel,task status is"));
            }
            // case2
            migrationTaskTbl.setStatus(MigrationStatusEnum.INIT.getStatus());
            dbMigrateService.cancelTask(1L);
            Mockito.verify(dbReplicationTblDao, Mockito.times(0)).update(Mockito.anyList());
            Mockito.verify(dBReplicationFilterMappingTblDao, Mockito.times(0)).update(Mockito.anyList());
            Mockito.verify(messengerGroupTblDao, Mockito.times(0)).update(Mockito.any(MessengerGroupTbl.class));
            Mockito.verify(messengerTblDao, Mockito.times(0)).update(Mockito.anyList());
            Mockito.verify(mhaReplicationTblDao, Mockito.never()).update(Mockito.any(MhaReplicationTbl.class));
            Mockito.verify(applierGroupTblV2Dao, Mockito.never()).update(Mockito.any(ApplierGroupTblV2.class));
            Mockito.verify(applierTblV2Dao, Mockito.never()).update(Mockito.anyList());
            Mockito.verify(mhaDbReplicationService, Mockito.times(0)).offlineMhaDbReplication(Mockito.anyList());

            // case3
            migrationTaskTbl.setStatus(MigrationStatusEnum.PRE_STARTED.getStatus());
            dbMigrateService.cancelTask(1L);
            Mockito.verify(dbReplicationTblDao, Mockito.times(2)).update(Mockito.anyList());
            Mockito.verify(dBReplicationFilterMappingTblDao, Mockito.times(2)).update(Mockito.anyList());
            Mockito.verify(messengerGroupTblDao, Mockito.times(1)).update(Mockito.any(MessengerGroupTbl.class));
            Mockito.verify(messengerTblDao, Mockito.times(1)).update(Mockito.anyList());
            Mockito.verify(mhaReplicationTblDao, Mockito.never()).update(Mockito.any(MhaReplicationTbl.class));
            Mockito.verify(applierGroupTblV2Dao, Mockito.never()).update(Mockito.any(ApplierGroupTblV2.class));
            Mockito.verify(applierTblV2Dao, Mockito.never()).update(Mockito.anyList());
            Mockito.verify(mhaDbReplicationService, Mockito.times(2)).offlineMhaDbReplication(Mockito.anyList());
            // case4
            
        }
    }

    private void initMock() throws Exception {
        Mockito.when(migrationTaskTblDao.queryById(Mockito.anyLong())).thenReturn(getMigrationTaskTbl());
        List<MhaTblV2> mhaTblV2s = getMhaTblV2s();
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("newMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("oldMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(1));
        Mockito.when(dbTblDao.queryByDbNames(Mockito.anyList())).thenReturn(getDbTbls());

        List<MhaDbMappingTbl> mhaDbMappingTbls = getMhaDbMappingTbls2();
        List<MhaDbMappingTbl> mhaDbMappingTbls1 = getMhaDbMappingTbls3();
        Map<Long, List<MhaDbMappingTbl>> mhaDbMappingTblsMap = mhaDbMappingTbls.stream().collect(Collectors.groupingBy(MhaDbMappingTbl::getMhaId));
        Map<Long, List<MhaDbMappingTbl>> mhaDbMappingTblsMap1 = mhaDbMappingTbls1.stream().collect(Collectors.groupingBy(MhaDbMappingTbl::getMhaId));

        Mockito.when(mhaDbMappingTblDao.queryByDbIds(Mockito.anyList())).thenReturn(mhaDbMappingTbls1);
        Mockito.when(mhaDbMappingTblDao.queryByMhaId(Mockito.eq(200L))).thenReturn(mhaDbMappingTblsMap.get(200L));
        Mockito.when(mhaDbMappingTblDao.queryByMhaId(Mockito.eq(201L))).thenReturn(mhaDbMappingTblsMap.get(201L));
        Mockito.when(mhaDbMappingTblDao.queryByMhaId(Mockito.eq(202L))).thenReturn(mhaDbMappingTblsMap.get(202L));

        List<Long> newMhaDbMappingIds = mhaDbMappingTblsMap.get(200L).stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
        List<Long> oldMhaDbMappingIds = mhaDbMappingTblsMap.get(201L).stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
        List<Long> relatedMhaDbMappingIds = mhaDbMappingTblsMap.get(202L).stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
        List<Long> newMhaDbMappingIds1 = mhaDbMappingTblsMap1.get(200L).stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
        List<Long> oldMhaDbMappingIds1 = mhaDbMappingTblsMap1.get(201L).stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
        List<Long> relatedMhaDbMappingIds1 = mhaDbMappingTblsMap1.get(202L).stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());

        Mockito.when(dbReplicationTblDao.queryBySrcMappingIds(newMhaDbMappingIds, 1)).thenReturn(new ArrayList<>());
        Mockito.when(dbReplicationTblDao.queryBySrcMappingIds(oldMhaDbMappingIds, 1)).thenReturn(new ArrayList<>());
        Mockito.when(dbReplicationTblDao.queryBySrcMappingIds(newMhaDbMappingIds1, 1)).thenReturn(getDbReplicationTbls());
        Mockito.when(dbReplicationTblDao.queryBySrcMappingIds(oldMhaDbMappingIds1, 1)).thenReturn(getDbReplicationTbls());
        Mockito.when(dbReplicationTblDao.queryByMappingIds(Mockito.anyList(), Mockito.anyList(), Mockito.anyInt())).thenReturn(getDbReplicationTbls());

        Mockito.when(dbReplicationTblDao.queryByMappingIds(oldMhaDbMappingIds1, relatedMhaDbMappingIds1, 0)).thenReturn(getDbReplicationTbls());
        Mockito.when(dbReplicationTblDao.queryByMappingIds(relatedMhaDbMappingIds1, oldMhaDbMappingIds1, 0)).thenReturn(getDbReplicationTbls());
        Mockito.when(dbReplicationTblDao.queryByMappingIds(oldMhaDbMappingIds, relatedMhaDbMappingIds, 0)).thenReturn(new ArrayList<>());
        Mockito.when(dbReplicationTblDao.queryByMappingIds(relatedMhaDbMappingIds, oldMhaDbMappingIds, 0)).thenReturn(new ArrayList<>());

        Mockito.when(dBReplicationFilterMappingTblDao.queryByDbReplicationIds(Mockito.anyList())).thenReturn(getFilterMappings());

        Mockito.when(messengerGroupTblDao.queryByMhaId(Mockito.anyLong(), Mockito.anyInt())).thenReturn(getMessengerGroup());
        Mockito.when(messengerTblDao.queryByGroupId(Mockito.anyLong())).thenReturn(Lists.newArrayList(getMessenger()));
        Mockito.when(messengerGroupTblDao.update(Mockito.any(MessengerGroupTbl.class))).thenReturn(1);
        Mockito.when(messengerTblDao.update(Mockito.anyList())).thenReturn(new int[1]);

        Mockito.when(dbReplicationTblDao.update(Mockito.anyList())).thenReturn(new int[1]);
        Mockito.when(dBReplicationFilterMappingTblDao.update(Mockito.anyList())).thenReturn(new int[1]);

        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyInt())).thenReturn(getMhaReplicationTbl());
        Mockito.when(mhaReplicationTblDao.update(Mockito.any(MhaReplicationTbl.class))).thenReturn(1);
        Mockito.when(applierGroupTblV2Dao.queryByMhaReplicationId(Mockito.anyLong(), Mockito.anyInt())).thenReturn(getApplierGroupTblV2s().get(0));
        Mockito.when(applierGroupTblV2Dao.update(Mockito.any(ApplierGroupTblV2.class))).thenReturn(1);
        Mockito.when(applierTblV2Dao.queryByApplierGroupId(Mockito.anyLong(), Mockito.anyInt())).thenReturn(getApplierTblV2s());
        Mockito.when(applierTblV2Dao.update(Mockito.anyList())).thenReturn(new int[1]);

        Mockito.when(mhaTblV2Dao.queryById(Mockito.anyLong())).thenReturn(mhaTblV2s.get(0));
        Mockito.when(dcTblDao.queryById(Mockito.anyLong())).thenReturn(getDcTbls().get(0));
        Mockito.when(metaGeneratorV3.getDrc()).thenReturn(getDrc());
        Map<String, String> urlMap = new HashMap<>();
        urlMap.put("sha", "url");
        Mockito.when(regionConfig.getCMRegionUrls()).thenReturn(urlMap);
        Mockito.when(migrationTaskTblDao.update(Mockito.any(MigrationTaskTbl.class))).thenReturn(1);
    }

    private Drc getDrc() {
        Drc drc = new Drc();
        Dc dc = new Dc("shaxy");
        DbCluster dbCluster = new DbCluster("cluster.mha200");
        dc.addDbCluster(dbCluster);
        drc.addDc(dc);
        return drc;
    }

    private MigrationTaskTbl getMigrationTaskTbl() {
        MigrationTaskTbl tbl = new MigrationTaskTbl();
        tbl.setId(1L);
        tbl.setOperator("operator");
        tbl.setNewMha("newMha");
        tbl.setOldMha("oldMha");
        tbl.setStatus(MigrationStatusEnum.READY_TO_SWITCH_DAL.getStatus());
        tbl.setDbs(JsonUtils.toJson(Lists.newArrayList("db")));
        return tbl;
    }


    @Test
    public void testGetAndUpdateTaskStatusDirectlyReturn() throws SQLException {
        MigrationTaskTbl tbl = new MigrationTaskTbl();
        tbl.setDbs(JsonUtils.toJson(Lists.newArrayList("db1")));
        List<String> possibleUpdateStatus = Lists.newArrayList(MigrationStatusEnum.STARTING.getStatus(), MigrationStatusEnum.READY_TO_SWITCH_DAL.getStatus());
        for (MigrationStatusEnum value : MigrationStatusEnum.values()) {
            tbl.setStatus(value.getStatus());
            Mockito.when(migrationTaskTblDao.queryById(1L)).thenReturn(tbl);
            if (!possibleUpdateStatus.contains(value.getStatus())) {
                String currentStatus = dbMigrateService.getAndUpdateTaskStatus(1L,true).getRight();
                Assert.assertEquals(value.getStatus(), currentStatus);
            }
        }
    }

    @Test
    public void testGetAndUPdateTaskStatusUpdateToReady() throws SQLException {
        MigrationTaskTbl tbl = new MigrationTaskTbl();
        tbl.setDbs(JsonUtils.toJson(Lists.newArrayList("db1")));
        tbl.setNewMha("mha1");
        tbl.setOldMha("mha2");


        List<String> possibleUpdateStatus = Lists.newArrayList(MigrationStatusEnum.STARTING.getStatus(), MigrationStatusEnum.READY_TO_SWITCH_DAL.getStatus());
        for (String status : possibleUpdateStatus) {
            tbl.setStatus(status);
            Mockito.when(migrationTaskTblDao.queryById(1L)).thenReturn(tbl);
            List<MhaReplicationDto> mhaReplication = this.getMhaReplication();
            Mockito.when(mhaReplicationServiceV2.queryRelatedReplications(Mockito.anyList(), Mockito.anyList())).thenReturn(mhaReplication);
            Mockito.when(mhaReplicationServiceV2.getMhaReplicationDelays(Mockito.anyList())).thenReturn(this.getSmallDelay(mhaReplication));
            Mockito.when(messengerServiceV2.getRelatedMhaMessenger(Mockito.anyList(), Mockito.anyList())).thenReturn(Lists.newArrayList());
            Mockito.when(messengerServiceV2.getMhaMessengerDelays(Mockito.anyList())).thenReturn(Lists.newArrayList());
            String currentStatus = dbMigrateService.getAndUpdateTaskStatus(1L,true).getRight();
            Assert.assertEquals(MigrationStatusEnum.READY_TO_SWITCH_DAL.getStatus(), currentStatus);
        }
        Mockito.verify(migrationTaskTblDao, Mockito.times(1)).update(Mockito.any(MigrationTaskTbl.class));
    }
    
    @Test
    public void testGetAndUPdateTaskStatusUpdateToStartting() throws SQLException {
        MigrationTaskTbl tbl = new MigrationTaskTbl();
        tbl.setDbs(JsonUtils.toJson(Lists.newArrayList("db1")));
        tbl.setNewMha("mha1");
        tbl.setOldMha("mha2");


        List<String> possibleUpdateStatus = Lists.newArrayList(MigrationStatusEnum.STARTING.getStatus(), MigrationStatusEnum.READY_TO_SWITCH_DAL.getStatus());
        for (String status : possibleUpdateStatus) {
            tbl.setStatus(status);
            Mockito.when(migrationTaskTblDao.queryById(1L)).thenReturn(tbl);
            List<MhaReplicationDto> mhaReplication = this.getMhaReplication();
            Mockito.when(mhaReplicationServiceV2.queryRelatedReplications(Mockito.anyList(), Mockito.anyList())).thenReturn(mhaReplication);
            Mockito.when(mhaReplicationServiceV2.getMhaReplicationDelays(Mockito.anyList())).thenReturn(this.getLargeDelay(mhaReplication));
            Mockito.when(messengerServiceV2.getRelatedMhaMessenger(Mockito.anyList(), Mockito.anyList())).thenReturn(Lists.newArrayList());
            Mockito.when(messengerServiceV2.getMhaMessengerDelays(Mockito.anyList())).thenReturn(Lists.newArrayList());
            String currentStatus = dbMigrateService.getAndUpdateTaskStatus(1L,true).getRight();
            Assert.assertEquals(MigrationStatusEnum.STARTING.getStatus(), currentStatus);
        }
        Mockito.verify(migrationTaskTblDao, Mockito.times(1)).update(Mockito.any(MigrationTaskTbl.class));
    }

    @Test
    public void getAndUpdateTaskStatusUpdateToReadyToCommitTask() throws SQLException {
        MigrationTaskTbl tbl = new MigrationTaskTbl();
        tbl.setDbs(JsonUtils.toJson(Lists.newArrayList("db1")));
        tbl.setNewMha("mha1");
        tbl.setOldMha("mha2");


        List<String> possibleUpdateStatus = Lists.newArrayList(MigrationStatusEnum.READY_TO_SWITCH_DAL.getStatus(), MigrationStatusEnum.READY_TO_COMMIT_TASK.getStatus());
        for (String status : possibleUpdateStatus) {
            tbl.setStatus(status);
            Mockito.when(migrationTaskTblDao.queryById(1L)).thenReturn(tbl);
            List<MhaReplicationDto> mhaReplication = this.getMhaReplication();
            Mockito.when(mhaReplicationServiceV2.queryRelatedReplications(Mockito.anyList(), Mockito.anyList())).thenReturn(mhaReplication);
            Mockito.when(mhaReplicationServiceV2.getMhaReplicationDelays(Mockito.anyList())).thenReturn(this.getSmallDelay(mhaReplication));
            Mockito.when(messengerServiceV2.getRelatedMhaMessenger(Mockito.anyList(), Mockito.anyList())).thenReturn(Lists.newArrayList());
            Mockito.when(messengerServiceV2.getMhaMessengerDelays(Mockito.anyList())).thenReturn(Lists.newArrayList());
            String currentStatus = dbMigrateService.getAndUpdateTaskStatus(1L,false).getRight();
            Assert.assertEquals(MigrationStatusEnum.READY_TO_COMMIT_TASK.getStatus(), currentStatus);
        }
        Mockito.verify(migrationTaskTblDao, Mockito.times(1)).update(Mockito.any(MigrationTaskTbl.class));
    }
    
    @Test
    public void getAndUpdateTaskStatusUpdateBackToReadyToSwitchDal() throws SQLException {
        MigrationTaskTbl tbl = new MigrationTaskTbl();
        tbl.setDbs(JsonUtils.toJson(Lists.newArrayList("db1")));
        tbl.setNewMha("mha1");
        tbl.setOldMha("mha2");


        List<String> possibleUpdateStatus = Lists.newArrayList(MigrationStatusEnum.READY_TO_SWITCH_DAL.getStatus(), MigrationStatusEnum.READY_TO_COMMIT_TASK.getStatus());
        for (String status : possibleUpdateStatus) {
            tbl.setStatus(status);
            Mockito.when(migrationTaskTblDao.queryById(1L)).thenReturn(tbl);
            List<MhaReplicationDto> mhaReplication = this.getMhaReplication();
            Mockito.when(mhaReplicationServiceV2.queryRelatedReplications(Mockito.anyList(), Mockito.anyList())).thenReturn(mhaReplication);
            Mockito.when(mhaReplicationServiceV2.getMhaReplicationDelays(Mockito.anyList())).thenReturn(this.getLargeDelay(mhaReplication));
            Mockito.when(messengerServiceV2.getRelatedMhaMessenger(Mockito.anyList(), Mockito.anyList())).thenReturn(Lists.newArrayList());
            Mockito.when(messengerServiceV2.getMhaMessengerDelays(Mockito.anyList())).thenReturn(Lists.newArrayList());
            String currentStatus = dbMigrateService.getAndUpdateTaskStatus(1L,false).getRight();
            Assert.assertEquals(MigrationStatusEnum.READY_TO_SWITCH_DAL.getStatus(), currentStatus);
        }
        Mockito.verify(migrationTaskTblDao, Mockito.times(1)).update(Mockito.any(MigrationTaskTbl.class));
    }
        
    
    
    @Test
    public void testPreStartReplicator() throws Exception {
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("mha200"), Mockito.anyInt())).thenReturn(PojoBuilder.getMhaTblV2s().get(0));
        Mockito.when(dcTblDao.queryById(Mockito.anyLong())).thenReturn(getDcTbl());
        Mockito.when(drcBuildServiceV2.syncMhaInfoFormDbaApi(Mockito.eq("mha201"))).thenReturn(PojoBuilder.getMhaTblV2s().get(1));
        Mockito.when(mhaTblV2Dao.update(Mockito.any(MhaTblV2.class))).thenReturn(1);
        Mockito.when(mysqlServiceV2.getMhaExecutedGtid(Mockito.eq("mha201"))).thenReturn("gtid");
        Mockito.when(resourceTblDao.queryAllExist()).thenReturn(PojoBuilder.getResourceTbls());
        List<ResourceView> resourceViews = MockEntityBuilder.buildResourceViews(2, ModuleEnum.REPLICATOR.getCode());
        Mockito.when(resourceService.autoConfigureResource(Mockito.any())).thenReturn(resourceViews);
        Mockito.when(drcBuildServiceV2.configureReplicatorGroup(Mockito.any(), Mockito.anyString(), Mockito.anyList(), Mockito.anyList())).thenReturn(1L);
        Mockito.when(consoleConfig.getConfgiCheckSwitch()).thenReturn(true);
        Mockito.when(mysqlServiceV2.preCheckMySqlConfig(Mockito.eq("mha200"))).thenReturn(new HashMap<>(){
            {
                put("gtid_mode", "ON");
                put("binlogTransactionDependencyHistorySize", 10000);
            }
        });
        Mockito.when(mysqlServiceV2.preCheckMySqlConfig(Mockito.eq("mha201"))).thenReturn(new HashMap<>(){
            {
                put("gtid_mode", "ON");
                put("binlogTransactionDependencyHistorySize", 5000);
            }
        });

        dbMigrateService.preStartReplicator("mha201", "mha200");
        Mockito.verify(drcBuildServiceV2, Mockito.times(1)).configureReplicatorGroup(Mockito.any(), Mockito.anyString(), Mockito.anyList(), Mockito.anyList());
        Mockito.when(mysqlServiceV2.preCheckMySqlConfig(Mockito.eq("mha200"))).thenReturn(new HashMap<>(){
            {
                put("gtid_mode", "OFF");
                put("autoIncrementOffset", 4);
            }
        });
        Mockito.when(mysqlServiceV2.preCheckMySqlConfig(Mockito.eq("mha201"))).thenReturn(new HashMap<>(){
            {
                put("gtid_mode", "ON");
                put("autoIncrementOffset", 3);
            }
        });
        try {
            dbMigrateService.preStartReplicator("mha201", "mha200");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("MhaConfigs not equals!"));
        }

    }

    @Test
    public void testMigrateMhaReplication() throws Exception {
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("mha200"), Mockito.anyInt())).thenReturn(PojoBuilder.getMhaTblV2s().get(0));
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("mha201"), Mockito.anyInt())).thenReturn(PojoBuilder.getMhaTblV2s().get(1));
        Mockito.when(mhaTblV2Dao.update(Mockito.any(MhaTblV2.class))).thenReturn(1);
        Mockito.when(mhaDbMappingTblDao.queryByMhaId(Mockito.eq(200L))).thenReturn(PojoBuilder.getMhaDbMappingTbls1().stream().filter(e -> e.getMhaId() == 200L).collect(Collectors.toList()));
        Mockito.when(mhaReplicationTblDao.queryByRelatedMhaId(Mockito.eq(Lists.newArrayList(200L)))).thenReturn(PojoBuilder.getMhaReplicationTbls2());
        Mockito.when(mhaReplicationTblDao.update(Mockito.anyList())).thenReturn(new int[1]);
        Mockito.when(mysqlServiceV2.getMhaExecutedGtid(Mockito.eq("mha201"))).thenReturn("gtid");

        List<ResourceView> resourceViews = MockEntityBuilder.buildResourceViews(2, ModuleEnum.APPLIER.getCode());
        Mockito.when(resourceService.autoConfigureResource(Mockito.any())).thenReturn(resourceViews);
        Mockito.when(applierGroupTblV2Dao.queryByMhaReplicationId(Mockito.anyLong(), Mockito.anyInt())).thenReturn(PojoBuilder.getApplierGroupTblV2s().get(0));
        Mockito.when(applierTblV2Dao.queryByApplierGroupId(Mockito.anyLong(), Mockito.anyInt())).thenReturn(PojoBuilder.getApplierTblV2s());
        Mockito.when(applierGroupTblV2Dao.update(Mockito.anyList())).thenReturn(new int[1]);
        Mockito.when(applierTblV2Dao.update(Mockito.anyList())).thenReturn(new int[1]);

        Mockito.when(mhaDbReplicationTblDao.queryByMhaDbMappingIds(Mockito.anyList())).thenReturn(PojoBuilder.getMhaDbReplicationTbls01());
        Mockito.when(dbApplierGroupTblDao.queryByMhaDbReplicationIds(Mockito.anyList())).thenReturn(PojoBuilder.getApplierGroupTblV3s());
        Mockito.when(dbApplierTblDao.queryByApplierGroupId(Mockito.anyLong(), Mockito.anyInt())).thenReturn(PojoBuilder.getApplierTblV3s());
        Mockito.when(dbApplierGroupTblDao.update(Mockito.anyList())).thenReturn(new int[1]);
        Mockito.when(dbApplierTblDao.update(Mockito.anyList())).thenReturn(new int[1]);

        Mockito.when(replicatorGroupTblDao.queryByMhaId(Mockito.eq(201L), Mockito.anyInt())).thenReturn(PojoBuilder.getReplicatorGroupTbls().get(0));
        Mockito.when(messengerGroupTblDao.queryByMhaId(Mockito.eq(200L), Mockito.anyInt())).thenReturn(PojoBuilder.getMessengerGroup());
        Mockito.when(messengerTblDao.queryByGroupIds(Mockito.anyList())).thenReturn(Lists.newArrayList(PojoBuilder.getMessengers()));
        Mockito.when(messengerGroupTblDao.update(Mockito.any(MessengerGroupTbl.class))).thenReturn(1);
        Mockito.when(messengerTblDao.update(Mockito.anyList())).thenReturn(new int[1]);
        Mockito.when(consoleConfig.getSgpMessengerGtidInit(Mockito.eq("mha201"))).thenReturn("messengerGtidInit");

        dbMigrateService.migrateMhaReplication("mha201", "mha200");
        Mockito.verify(applierGroupTblV2Dao, Mockito.times(1)).update(Mockito.any(ApplierGroupTblV2.class));
        Mockito.verify(applierTblV2Dao, Mockito.times(1)).update(Mockito.anyList());
        Mockito.verify(dbApplierGroupTblDao, Mockito.times(1)).update(Mockito.anyList());
        Mockito.verify(dbApplierTblDao, Mockito.times(1)).update(Mockito.anyList());
        Mockito.verify(messengerGroupTblDao, Mockito.times(1)).update(Mockito.any(MessengerGroupTbl.class));
        Mockito.verify(messengerTblDao, Mockito.times(1)).update(Mockito.anyList());
    }

    private List<MhaReplicationDto> getMhaReplication() {
        String json = "[{\"replicationId\":1,\"srcMha\":{\"name\":\"mha1\",\"id\":1},\"dstMha\":{\"name\":\"mha2\",\"id\":2},\"dbs\":[\"db1\",\"db2\"],\"status\":1}]";
        return JSON.parseArray(json, MhaReplicationDto.class);
    }

    private List<MhaDelayInfoDto> getSmallDelay(List<MhaReplicationDto> mhaReplicationDtos){
        return mhaReplicationDtos.stream().map(e->{
            MhaDelayInfoDto dto = new MhaDelayInfoDto();
            dto.setSrcMha(e.getSrcMha().getName());
            dto.setDstMha(e.getDstMha().getName());
            long delay = new Random().nextInt((int) TimeUnit.SECONDS.toMillis(5));
            dto.setDstTime(new Date().getTime());
            dto.setSrcTime(dto.getDstTime() + delay);
            return dto;
        }).collect(Collectors.toList());
    }

    private List<MhaDelayInfoDto> getLargeDelay(List<MhaReplicationDto> mhaReplicationDtos){
        return mhaReplicationDtos.stream().map(e->{
            MhaDelayInfoDto dto = new MhaDelayInfoDto();
            dto.setSrcMha(e.getSrcMha().getName());
            dto.setDstMha(e.getDstMha().getName());
            long delay = TimeUnit.SECONDS.toMillis(20);
            dto.setDstTime(new Date().getTime());
            dto.setSrcTime(dto.getDstTime() + delay);
            return dto;
        }).collect(Collectors.toList());
    }

    private DcTbl getDcTbl() {
        DcTbl dcTbl = new DcTbl();
        dcTbl.setRegionName("sgp");
        return dcTbl;
    }
}
