package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplierTblV2;
import com.ctrip.framework.drc.console.dao.v2.ApplierGroupTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.ApplierTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.MhaReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceBuildParam;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceQueryParam;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceSelectParam;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceServiceImpl;
import com.ctrip.framework.drc.console.vo.v2.MhaReplicationView;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.ctrip.framework.drc.core.http.PageReq;
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
 * 2023/8/18 2:07 下午
 */
public class ResourceServiceTest {

    @InjectMocks
    private ResourceServiceImpl resourceService;
    @Mock
    private ResourceTblDao resourceTblDao;
    @Mock
    private ReplicatorTblDao replicatorTblDao;
    @Mock
    private ApplierTblV2Dao applierTblDao;
    @Mock
    private DcTblDao dcTblDao;
    @Mock
    private MhaTblV2Dao mhaTblV2Dao;
    @Mock
    private MessengerTblDao messengerTblDao;
    @Mock
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Mock
    private ApplierGroupTblV2Dao applierGroupTblDao;
    @Mock
    private MhaReplicationTblDao mhaReplicationTblDao;
    
    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testConfigureResource() throws Exception {
        Mockito.when(dcTblDao.queryByDcName(Mockito.anyString())).thenReturn(getDcTbls().get(0));
        Mockito.when(resourceTblDao.queryByIp(Mockito.anyString())).thenReturn(null);
        Mockito.when(resourceTblDao.insert(Mockito.any(ResourceTbl.class))).thenReturn(0);

        ResourceBuildParam param = new ResourceBuildParam();
        param.setAz("AZ");
        param.setIp("127.0.0.1");
        param.setDcName("dc");
        param.setType("R");
        param.setTag("tag");
        resourceService.configureResource(param);
        Mockito.verify(resourceTblDao, Mockito.times(1)).insert(Mockito.any(ResourceTbl.class));
        Mockito.verify(resourceTblDao, Mockito.never()).update(Mockito.any(ResourceTbl.class));
    }

    @Test
    public void testOfflineResource() throws Exception {
        Mockito.when(resourceTblDao.queryByPk(Mockito.anyLong())).thenReturn(getResourceTbls().get(0));
        Mockito.when(replicatorTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        Mockito.when(applierTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        Mockito.when(resourceTblDao.update(Mockito.any(ResourceTbl.class))).thenReturn(0);

        resourceService.offlineResource(1L);
        Mockito.verify(resourceTblDao, Mockito.times(1)).update(Mockito.any(ResourceTbl.class));
    }

    @Test(expected = ConsoleException.class)
    public void testOfflineResource01() throws Exception {
        Mockito.when(resourceTblDao.queryByPk(Mockito.anyLong())).thenReturn(getResourceTbls().get(0));
        Mockito.when(replicatorTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(Lists.newArrayList(new ReplicatorTbl()));
        Mockito.when(resourceTblDao.update(Mockito.any(ResourceTbl.class))).thenReturn(0);

        resourceService.offlineResource(1L);
    }

    @Test(expected = ConsoleException.class)
    public void testOfflineResource02() throws Exception {
        ResourceTbl resourceTbl = new ResourceTbl();
        resourceTbl.setType(1);
        Mockito.when(resourceTblDao.queryByPk(Mockito.anyLong())).thenReturn(resourceTbl);
        Mockito.when(resourceTblDao.update(Mockito.any(ResourceTbl.class))).thenReturn(0);
        Mockito.when(applierTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(Lists.newArrayList(new ApplierTblV2()));

        resourceService.offlineResource(1L);
    }

    @Test
    public void testOnlineResource() throws Exception {
        Mockito.when(resourceTblDao.queryByPk(Mockito.anyLong())).thenReturn(getResourceTbls().get(0));
        Mockito.when(resourceTblDao.update(Mockito.any(ResourceTbl.class))).thenReturn(0);
        resourceService.onlineResource(1L);

        Mockito.verify(resourceTblDao, Mockito.times(1)).update(Mockito.any(ResourceTbl.class));
    }

    @Test
    public void testDeactivateResource() throws Exception {
        Mockito.when(resourceTblDao.queryByPk(Mockito.anyLong())).thenReturn(getResourceTbls().get(0));
        Mockito.when(resourceTblDao.update(Mockito.any(ResourceTbl.class))).thenReturn(0);
        resourceService.deactivateResource(1L);

        Mockito.verify(resourceTblDao, Mockito.times(1)).update(Mockito.any(ResourceTbl.class));
    }

    @Test
    public void testRecoverResource() throws Exception {
        Mockito.when(resourceTblDao.queryByPk(Mockito.anyLong())).thenReturn(getResourceTbls().get(0));
        Mockito.when(resourceTblDao.update(Mockito.any(ResourceTbl.class))).thenReturn(0);
        resourceService.recoverResource(1L);

        Mockito.verify(resourceTblDao, Mockito.times(1)).update(Mockito.any(ResourceTbl.class));
    }

    @Test
    public void testGetResourceView() throws Exception {
        ResourceQueryParam param = new ResourceQueryParam();
        param.setPageReq(new PageReq());
        Mockito.when(resourceTblDao.queryByParam(param)).thenReturn(getResourceTbls());
        Mockito.when(replicatorTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        Mockito.when(applierTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(new ArrayList<>());

        List<ResourceView> result = resourceService.getResourceView(param);
        Assert.assertEquals(result.size(), getResourceTbls().size());

        param.setRegion("region");
        result = resourceService.getResourceView(param);
        Assert.assertEquals(result.size(), 0);
    }

    @Test
    public void testGetMhaAvailableResource() throws Exception {
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("mha"), Mockito.anyInt())).thenReturn(getMhaTblV2());
        Mockito.when(dcTblDao.queryById(Mockito.anyLong())).thenReturn(getDcTbls().get(0));
        Mockito.when(dcTblDao.queryByRegionName(Mockito.anyString())).thenReturn(getDcTbls());
        Mockito.when(resourceTblDao.queryByDcAndTag(Mockito.anyList(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(getResourceTbls());
        Mockito.when(replicatorTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        Mockito.when(applierTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        Mockito.when(messengerTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(new ArrayList<>());

        List<ResourceView> result = resourceService.getMhaAvailableResource("mha", 0);
        Assert.assertEquals(result.size(), getResourceTbls().size());
    }

    @Test
    public void testAutoConfigureResource() throws Exception {
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("mha"), Mockito.anyInt())).thenReturn(getMhaTblV2());
        Mockito.when(dcTblDao.queryById(Mockito.anyLong())).thenReturn(getDcTbls().get(0));
        Mockito.when(dcTblDao.queryByRegionName(Mockito.anyString())).thenReturn(getDcTbls());
        Mockito.when(resourceTblDao.queryByDcAndTag(Mockito.anyList(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(getResourceTbls());
        Mockito.when(replicatorTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        Mockito.when(applierTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        Mockito.when(messengerTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(new ArrayList<>());

        ResourceSelectParam param = new ResourceSelectParam();
        param.setMhaName("mha");
        param.setType(0);

        List<ResourceView> result = resourceService.autoConfigureResource(param);
        Assert.assertEquals(result.size(), getResourceTbls().size());
    }

    @Test
    public void testHandOffResource() throws Exception {
        Mockito.when(mhaTblV2Dao.queryByMhaName(Mockito.eq("mha"), Mockito.anyInt())).thenReturn(getMhaTblV2());
        Mockito.when(dcTblDao.queryById(Mockito.anyLong())).thenReturn(getDcTbls().get(0));
        Mockito.when(dcTblDao.queryByRegionName(Mockito.anyString())).thenReturn(getDcTbls());
        Mockito.when(resourceTblDao.queryByDcAndTag(Mockito.anyList(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(getResourceTbls());
        Mockito.when(replicatorTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        Mockito.when(applierTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        Mockito.when(messengerTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(new ArrayList<>());

        ResourceSelectParam param = new ResourceSelectParam();
        param.setMhaName("mha");
        param.setType(0);
        param.setSelectedIps(Lists.newArrayList("ip1"));

        List<ResourceView> result = resourceService.handOffResource(param);
        Assert.assertEquals(result.size(), getResourceTbls().size());
    }

    @Test
    public void testQueryMhaByReplicator () throws Exception {
        Mockito.when(replicatorTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(getReplicatorTbls());
        Mockito.when(replicatorGroupTblDao.queryByIds(Mockito.anyList())).thenReturn(getReplicatorGroupTbls());
        Mockito.when(mhaTblV2Dao.queryByIds(Mockito.anyList())).thenReturn(getMhaTblV2s());

        List<String> result = resourceService.queryMhaByReplicator(1L);
        Assert.assertEquals(result.size(), getMhaTblV2s().size());
    }

    @Test
    public void testQueryMhaReplicationByApplier () throws Exception {
        Mockito.when(applierTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(getApplierTblV2s());
        Mockito.when(applierGroupTblDao.queryByIds(Mockito.anyList())).thenReturn(getApplierGroupTblV2s());
        Mockito.when(mhaTblV2Dao.queryByIds(Mockito.anyList())).thenReturn(getMhaTblV2s());
        Mockito.when(mhaReplicationTblDao.queryByIds(Mockito.anyList())).thenReturn(getMhaReplicationTbls());
        Mockito.when(dcTblDao.queryAllExist()).thenReturn(getDcTbls());

        List<MhaReplicationView> result = resourceService.queryMhaReplicationByApplier(1L);
        Assert.assertEquals(result.size(), getMhaReplicationTbls().size());
    }

}
