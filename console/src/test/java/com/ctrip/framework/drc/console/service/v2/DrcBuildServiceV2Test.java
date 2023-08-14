package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.param.v2.*;
import com.ctrip.framework.drc.console.service.v2.impl.DrcBuildServiceV2Impl;
import com.ctrip.framework.drc.console.vo.v2.ColumnsConfigView;
import com.ctrip.framework.drc.console.vo.v2.DbReplicationView;
import com.ctrip.framework.drc.console.vo.v2.RowsFilterConfigView;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static com.ctrip.framework.drc.console.service.v2.MigrateEntityBuilder.*;

/**
 * Created by dengquanliang
 * 2023/8/11 21:58
 */
public class DrcBuildServiceV2Test {
    @InjectMocks
    private DrcBuildServiceV2Impl drcBuildServiceV2;
    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;
    @Mock
    private MetaInfoServiceV2 metaInfoService;
    @Mock
    private MhaDbMappingService mhaDbMappingService;
    @Mock
    private MhaTblV2Dao mhaTblDao;
    @Mock
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Mock
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Mock
    private ReplicatorTblDao replicatorTblDao;
    @Mock
    private ApplierGroupTblV2Dao applierGroupTblDao;
    @Mock
    private ApplierTblV2Dao applierTblDao;
    @Mock
    private ResourceTblDao resourceTblDao;
    @Mock
    private DbTblDao dbTblDao;
    @Mock
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Mock
    private DbReplicationTblDao dbReplicationTblDao;
    @Mock
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Mock
    private ColumnsFilterTblV2Dao columnFilterTblV2Dao;
    @Mock
    private RowsFilterTblV2Dao rowsFilterTblV2Dao;
    @Mock
    private BuTblDao buTblDao;
    @Mock
    private DcTblDao dcTblDao;
    @Mock
    private CacheMetaService cacheMetaService;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testBuildMha() throws Exception {
        Mockito.when(buTblDao.queryByBuName(Mockito.anyString())).thenReturn(getBuTbl());
        Mockito.when(dcTblDao.queryByDcName(Mockito.anyString())).thenReturn(getDcTbls().get(0));
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.anyString())).thenReturn(null);
        Mockito.when(mhaTblDao.insertWithReturnId(Mockito.any(MhaTblV2.class))).thenReturn(1L);
        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyInt())).thenReturn(null);
        Mockito.when(mhaReplicationTblDao.insert(Mockito.any(MhaReplicationTbl.class))).thenReturn(1);

        drcBuildServiceV2.buildMha(new DrcMhaBuildParam("srcMha", "dstMha", "srcDc", "dstDc", "BBZ"));
        Mockito.verify(mhaTblDao, Mockito.never()).update(Mockito.any(MhaTblV2.class));
    }

    @Test
    public void testBuildDrc() throws Exception {
        DrcBuildBaseParam srcParam = new DrcBuildBaseParam("srcMha", Lists.newArrayList("127.0.0.1"),
                Lists.newArrayList("127.0.0.1"), "rGtid", "aGtid");
        DrcBuildBaseParam dstParam = new DrcBuildBaseParam("dstMha", Lists.newArrayList("127.0.0.1"),
                Lists.newArrayList("127.0.0.1"), "rGtid", "aGtid");
        DrcBuildParam param = new DrcBuildParam();
        param.setSrcBuildParam(srcParam);
        param.setDstBuildParam(dstParam);
        List<MhaTblV2> mhaTblV2s = getMhaTblV2s();

        Mockito.when(mhaTblDao.queryByMhaName(Mockito.eq("srcMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.eq("dstMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        Mockito.when(mhaTblDao.update(Mockito.any(MhaTblV2.class))).thenReturn(1);

        Mockito.when(replicatorGroupTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(null);
        Mockito.when(replicatorGroupTblDao.insertWithReturnId(Mockito.any())).thenReturn(200L);

        Mockito.when(applierGroupTblDao.queryByMhaReplicationId(Mockito.anyLong())).thenReturn(null);
        Mockito.when(applierGroupTblDao.insertWithReturnId(Mockito.any())).thenReturn(200L);

        Mockito.when(replicatorTblDao.queryByRGroupIds(Mockito.anyList(), Mockito.anyInt())).thenReturn(new ArrayList<>());
        Mockito.when(replicatorTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[1]);

        Mockito.when(applierTblDao.queryByApplierGroupId(Mockito.anyLong(), Mockito.anyInt())).thenReturn(new ArrayList<>());
        Mockito.when(applierTblDao.batchInsert(Mockito.anyList())).thenReturn(new int[1]);

        Mockito.when(resourceTblDao.queryByIps(Mockito.anyList())).thenReturn(getResourceTbls());
        Mockito.when(mhaReplicationTblDao.insertWithReturnId(Mockito.any())).thenReturn(200L);
        Mockito.when(mhaReplicationTblDao.insert(Mockito.any(MhaReplicationTbl.class))).thenReturn(1);
        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyInt())).thenReturn(getMhaReplicationTbls().get(0));
        Mockito.when(mhaReplicationTblDao.update(Mockito.any(MhaReplicationTbl.class))).thenReturn(1);

        drcBuildServiceV2.buildDrc(param);

        Mockito.verify(replicatorTblDao, Mockito.never()).batchUpdate(Mockito.anyList());
        Mockito.verify(replicatorTblDao, Mockito.times(2)).batchInsert(Mockito.anyList());
        Mockito.verify(applierTblDao, Mockito.never()).batchUpdate(Mockito.anyList());
        Mockito.verify(applierTblDao, Mockito.times(2)).batchInsert(Mockito.anyList());

        Mockito.verify(replicatorGroupTblDao, Mockito.times(2)).insertWithReturnId(Mockito.any());
        Mockito.verify(replicatorGroupTblDao, Mockito.never()).update(Mockito.anyList());
        Mockito.verify(applierGroupTblDao, Mockito.times(2)).insertWithReturnId(Mockito.any());
        Mockito.verify(applierGroupTblDao, Mockito.never()).update(Mockito.anyList());

        Mockito.verify(mhaReplicationTblDao, Mockito.times(2)).update(Mockito.any(MhaReplicationTbl.class));
        Mockito.verify(mhaTblDao, Mockito.times(2)).update(Mockito.any(MhaTblV2.class));
    }

    @Test
    public void testConfigureDbReplications() throws Exception {
        DbReplicationBuildParam param = new DbReplicationBuildParam("srcMha", "dstMha", "db", "table");

        List<MhaTblV2> mhaTblV2s = getMhaTblV2s();
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.eq("srcMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.eq("dstMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));

        Mockito.when(mhaDbMappingService.buildMhaDbMappings(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(Lists.newArrayList("db200"));

        Mockito.when(mhaDbMappingTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(getMhaDbMappingTbls1());
        Mockito.when(dbTblDao.queryByDbNames(Mockito.anyList())).thenReturn(getDbTbls());
        Mockito.when(dbReplicationTblDao.queryByMappingIds(Mockito.anyList(), Mockito.anyList(), Mockito.anyInt())).thenReturn(new ArrayList<>());
        Mockito.doNothing().when(dbReplicationTblDao).batchInsertWithReturnId(Mockito.anyList());

        List<Long> results = drcBuildServiceV2.configureDbReplications(param);
        Assert.assertNotNull(results);
        Mockito.verify(dbReplicationTblDao, Mockito.times(1)).batchInsertWithReturnId(Mockito.any());

    }

    @Test
    public void testGetDbReplicationView() throws Exception {
        List<MhaTblV2> mhaTblV2s = getMhaTblV2s();
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.eq("srcMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.eq("dstMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));

        Mockito.when(mhaDbMappingTblDao.queryByMhaId(Mockito.anyLong())).thenReturn(getMhaDbMappingTbls1());
        Mockito.when(dbReplicationTblDao.queryByMappingIds(Mockito.anyList(), Mockito.anyList(), Mockito.anyInt())).thenReturn(getDbReplicationTbls());
        Mockito.when(dbTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbTbls());

        List<DbReplicationView> result = drcBuildServiceV2.getDbReplicationView("srcMha", "dstMha");
        Assert.assertEquals(result.size(), 2);
    }

    @Test
    public void testDeleteDbReplications() throws Exception {
        Mockito.when(dbReplicationTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbReplicationTbls());
        Mockito.when(dbReplicationTblDao.batchUpdate(Mockito.anyList())).thenReturn(new int[1]);
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationId(Mockito.anyLong())).thenReturn(new ArrayList<>());
        Mockito.when(dbReplicationFilterMappingTblDao.batchUpdate(Mockito.anyList())).thenReturn(new int[1]);

        drcBuildServiceV2.deleteDbReplications(200L);
        Mockito.verify(dbReplicationTblDao, Mockito.times(1)).batchUpdate(Mockito.any());
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.never()).batchUpdate(Mockito.any());
    }

    @Test
    public void testBuildColumnsFilter() throws Exception {
        ColumnsFilterCreateParam param = new ColumnsFilterCreateParam(Lists.newArrayList(200L, 201L), 0, Lists.newArrayList("column"));
        Mockito.when(dbReplicationTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbReplicationTbls());
        Mockito.when(columnFilterTblV2Dao.queryByMode(Mockito.anyInt())).thenReturn(new ArrayList<>());
        Mockito.when(columnFilterTblV2Dao.insertReturnId(Mockito.any())).thenReturn(200L);
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationId(Mockito.anyLong())).thenReturn(new ArrayList<>());

        drcBuildServiceV2.buildColumnsFilter(param);
        Mockito.verify(columnFilterTblV2Dao, Mockito.times(1)).insertReturnId(Mockito.any());
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.times(1)).batchInsert(Mockito.anyList());
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.never()).batchUpdate(Mockito.anyList());

    }

    @Test
    public void testGetColumnsConfigView() throws Exception {
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationId(Mockito.anyLong())).thenReturn(getFilterMappings());
        Mockito.when(columnFilterTblV2Dao.queryById(Mockito.anyLong())).thenReturn(getColumnsFilterTbl());

        ColumnsConfigView result = drcBuildServiceV2.getColumnsConfigView(200L);
        Assert.assertEquals(result.getColumns().size(), 1);
    }

    @Test
    public void testDeleteColumnsFilter() throws Exception {
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Mockito.anyList())).thenReturn(getFilterMappings());
        Mockito.when(dbReplicationTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbReplicationTbls());
        Mockito.when(dbReplicationFilterMappingTblDao.batchUpdate(Mockito.anyList())).thenReturn(new int[1]);

        drcBuildServiceV2.deleteColumnsFilter(Lists.newArrayList(200L, 201L));
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.times(1)).batchUpdate(Mockito.anyList());
    }

    @Test
    public void testGetRowsConfigView() throws Exception {
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationId(Mockito.anyLong())).thenReturn(getFilterMappings());
        Mockito.when(rowsFilterTblV2Dao.queryById(Mockito.anyLong())).thenReturn(getRowsFilterTbl());

        RowsFilterConfigView result = drcBuildServiceV2.getRowsConfigView(200L);
        Assert.assertEquals(result.getColumns().size(), 1);
        Assert.assertEquals(result.getUdlColumns().size(), 1);
        Assert.assertEquals(result.getMode(), 1);
    }

    @Test
    public void testDeleteRowsFilter() throws Exception {
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationIds(Mockito.anyList())).thenReturn(getFilterMappings());
        Mockito.when(dbReplicationTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbReplicationTbls());
        Mockito.when(dbReplicationFilterMappingTblDao.batchUpdate(Mockito.anyList())).thenReturn(new int[1]);

        drcBuildServiceV2.deleteRowsFilter(Lists.newArrayList(200L, 201L));
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.times(1)).batchUpdate(Mockito.anyList());
    }

    @Test
    public void testBuildRowsFilter() throws Exception {
        RowsFilterCreateParam param = getRowsFilterCreateParam();
        Mockito.when(dbReplicationTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbReplicationTbls());
        Mockito.when(rowsFilterTblV2Dao.queryByMode(Mockito.anyInt())).thenReturn(new ArrayList<>());
        Mockito.when(rowsFilterTblV2Dao.insertReturnId(Mockito.any())).thenReturn(200L);
        Mockito.when(dbReplicationFilterMappingTblDao.queryByDbReplicationId(Mockito.anyLong())).thenReturn(new ArrayList<>());

        drcBuildServiceV2.buildRowsFilter(param);
        Mockito.verify(rowsFilterTblV2Dao, Mockito.times(1)).insertReturnId(Mockito.any());
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.times(1)).batchInsert(Mockito.anyList());
        Mockito.verify(dbReplicationFilterMappingTblDao, Mockito.never()).batchUpdate(Mockito.anyList());
    }

    @Test
    public void testGetApplierGtid() throws Exception {
        List<MhaTblV2> mhaTblV2s = getMhaTblV2s();
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.eq("srcMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        Mockito.when(mhaTblDao.queryByMhaName(Mockito.eq("dstMha"), Mockito.anyInt())).thenReturn(mhaTblV2s.get(0));
        Mockito.when(mhaReplicationTblDao.queryByMhaId(Mockito.anyLong(), Mockito.anyLong())).thenReturn(getMhaReplicationTbl());
        Mockito.when(applierGroupTblDao.queryByMhaReplicationId(Mockito.anyLong(), Mockito.anyInt())).thenReturn(getApplierGroupTblV2s().get(0));

        String result = drcBuildServiceV2.getApplierGtid("srcMha", "dstMha");
        Assert.assertEquals(result, getApplierGroupTblV2s().get(0).getGtidInit());
    }
}
