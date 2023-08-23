package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.MessengerGroupTblDao;
import com.ctrip.framework.drc.console.dao.MessengerTblDao;
import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.enums.MigrationStatusEnum;
import com.ctrip.framework.drc.console.service.v2.impl.DbMigrateServiceImpl;
import com.ctrip.framework.drc.console.service.v2.impl.MetaGeneratorV3;
import com.ctrip.framework.drc.core.config.RegionConfig;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.service.v2.MigrateEntityBuilder.*;

/**
 * Created by dengquanliang
 * 2023/8/23 15:03
 */
public class DbMigrateServiceTest {

    @InjectMocks
    private DbMigrateServiceImpl dbMigrateService;
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

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

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

    }

    @Test
    public void testOfflineOldDrcConfig() throws Exception {
        try (MockedStatic<HttpUtils> mockedStatic = Mockito.mockStatic(HttpUtils.class)) {
            mockedStatic.when(() -> {
                HttpUtils.post(Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.any(Map.class));
            }).thenReturn(ApiResult.getSuccessInstance(null));
            dbMigrateService.offlineOldDrcConfig(1L);

            Mockito.verify(dbReplicationTblDao, Mockito.times(3)).update(Mockito.anyList());
            Mockito.verify(dBReplicationFilterMappingTblDao, Mockito.times(3)).update(Mockito.anyList());
            Mockito.verify(messengerGroupTblDao, Mockito.times(1)).update(Mockito.any(MessengerGroupTbl.class));
            Mockito.verify(messengerTblDao, Mockito.times(1)).update(Mockito.anyList());
            Mockito.verify(mhaReplicationTblDao, Mockito.times(2)).update(Mockito.any(MhaReplicationTbl.class));
            Mockito.verify(applierGroupTblV2Dao, Mockito.times(2)).update(Mockito.any(ApplierGroupTblV2.class));
            Mockito.verify(applierTblV2Dao, Mockito.times(2)).update(Mockito.anyList());
        }

    }

    @Test
    public void testRollBackNewDrcConfig() throws Exception {
        try (MockedStatic<HttpUtils> mockedStatic = Mockito.mockStatic(HttpUtils.class)) {
            mockedStatic.when(() -> {
                HttpUtils.post(Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.any(Map.class));
            }).thenReturn(ApiResult.getSuccessInstance(null));
            dbMigrateService.rollBackNewDrcConfig(1L);


            Mockito.verify(dbReplicationTblDao, Mockito.times(3)).update(Mockito.anyList());
            Mockito.verify(dBReplicationFilterMappingTblDao, Mockito.times(3)).update(Mockito.anyList());
            Mockito.verify(messengerGroupTblDao, Mockito.times(1)).update(Mockito.any(MessengerGroupTbl.class));
            Mockito.verify(messengerTblDao, Mockito.times(1)).update(Mockito.anyList());
            Mockito.verify(mhaReplicationTblDao, Mockito.times(2)).update(Mockito.any(MhaReplicationTbl.class));
            Mockito.verify(applierGroupTblV2Dao, Mockito.times(2)).update(Mockito.any(ApplierGroupTblV2.class));
            Mockito.verify(applierTblV2Dao, Mockito.times(2)).update(Mockito.anyList());

        }

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
        tbl.setOperator("operator");
        tbl.setNewMha("newMha");
        tbl.setOldMha("oldMha");
        tbl.setStatus(MigrationStatusEnum.READY_TO_SWITCH_DAL.getStatus());
        tbl.setDbs(JsonUtils.toJson(Lists.newArrayList("db")));
        return tbl;
    }

}
