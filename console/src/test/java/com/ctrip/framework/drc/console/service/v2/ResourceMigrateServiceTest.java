package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.ApplierTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.param.v2.resource.DeleteIpParam;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceBuildParam;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceQueryParam;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceMigrateServiceImpl;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
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
 * 2023/8/18 3:39 下午
 */
public class ResourceMigrateServiceTest {

    @InjectMocks
    private ResourceMigrateServiceImpl resourceMigrateService;
    @Mock
    private ResourceTblDao resourceTblDao;
    @Mock
    private ReplicatorTblDao replicatorTblDao;
    @Mock
    private ApplierTblV2Dao applierTblV2Dao;
    @Mock
    private DcTblDao dcTblDao;
    @Mock
    private MhaTblV2Dao mhaTblV2Dao;
    @Mock
    private MhaTblDao mhaTblDao;
    @Mock
    private ApplierGroupTblDao applierGroupTblDao;
    @Mock
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Mock
    private MessengerTblDao messengerTblDao;
    @Mock
    private ResourceService resourceService;
    
    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGetResourceUnused() throws Exception {
        Mockito.when(resourceService.getResourceView(Mockito.any(ResourceQueryParam.class))).thenReturn(new ArrayList<>());
        List<ResourceView> result = resourceMigrateService.getResourceUnused(0);
        Assert.assertEquals(result.size(), 0);
    }

    @Test
    public void testGetDeletedIps() {
        List<String> unusedIps = Lists.newArrayList("ip1", "ip2");
        List<String> existIps = Lists.newArrayList("ip2", "ip3");
        DeleteIpParam param = new DeleteIpParam();
        param.setUnusedIps(unusedIps);
        param.setExistIps(existIps);
        List<String> result = resourceMigrateService.getDeletedIps(param);
        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.get(0), "ip1");
    }

    @Test
    public void testDeleteResourceUnused() throws Exception {
        List<String> ips = Lists.newArrayList("ip");
        Mockito.when(resourceTblDao.queryByIps(ips)).thenReturn(getResourceTbls());
        Mockito.when(replicatorTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        Mockito.when(applierTblV2Dao.queryByResourceIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        Mockito.when(resourceTblDao.update(Mockito.anyList())).thenReturn(new int[1]);

        int result = resourceMigrateService.deleteResourceUnused(ips);
        Assert.assertEquals(result, 1);
    }

    @Test
    public void testUpdateResource() throws Exception {
        ResourceBuildParam param = new ResourceBuildParam();
        param.setIp("ip");
        param.setTag("tag");
        param.setAz("az");

        Mockito.when(resourceTblDao.queryByIps(Mockito.anyList())).thenReturn(getResourceTbls());
        Mockito.when(resourceTblDao.update(Mockito.anyList())).thenReturn(new int[1]);

        int result = resourceMigrateService.updateResource(Lists.newArrayList(param));
        Assert.assertEquals(result, 1);
        Mockito.verify(resourceTblDao, Mockito.times(1)).update(Mockito.anyList());
    }

    @Test
    public void updateMhaTag() throws Exception {
        List<MhaTblV2> mhaTblV2s = getMhaTblV2s();
        List<MhaTbl> mhaTbls = getMhaTbls();
        Mockito.when(mhaTblV2Dao.queryAllExist()).thenReturn(mhaTblV2s);
        Mockito.when(mhaTblDao.queryAllExist()).thenReturn(mhaTbls);
        Mockito.when(replicatorGroupTblDao.queryAllExist()).thenReturn(getReplicatorGroupTbls());
        Mockito.when(replicatorTblDao.queryAllExist()).thenReturn(getReplicatorTbls());
        Mockito.when(applierGroupTblDao.queryAllExist()).thenReturn(getApplierGroupTbls());
        Mockito.when(applierTblV2Dao.queryAllExist()).thenReturn(getApplierTblV2s());
        Mockito.when(resourceTblDao.queryAllExist()).thenReturn(getResourceTbls());

        resourceMigrateService.updateMhaTag();

        Mockito.verify(mhaTblV2Dao, Mockito.times(1)).update(Mockito.anyList());
        Mockito.verify(mhaTblDao, Mockito.times(1)).update(Mockito.anyList());

    }

    @Test
    public void testGetReplicatorGroupIdsWithSameAz() throws Exception {
        Mockito.when(replicatorTblDao.queryAllExist()).thenReturn(getReplicatorTbls());
        Mockito.when(resourceTblDao.queryAllExist()).thenReturn(getResourceTbls());

        List<Long> result = resourceMigrateService.getReplicatorGroupIdsWithSameAz();
        Assert.assertEquals(result.size(), 1);
    }

    @Test
    public void testGetApplierGroupIdsWithSameAz() throws Exception {
        Mockito.when(applierTblV2Dao.queryAllExist()).thenReturn(getApplierTblV2s());
        Mockito.when(resourceTblDao.queryAllExist()).thenReturn(getResourceTbls());

        List<Long> result = resourceMigrateService.getApplierGroupIdsWithSameAz();
        Assert.assertEquals(result.size(), 1);
    }

    @Test
    public void testGetMessengerGroupIdsWithSameAz() throws Exception {
        Mockito.when(messengerTblDao.queryAllExist()).thenReturn(getMessengers());
        Mockito.when(resourceTblDao.queryAllExist()).thenReturn(getResourceTbls());

        List<Long> result = resourceMigrateService.getMessengerGroupIdsWithSameAz();
        Assert.assertEquals(result.size(), 1);
    }


}
