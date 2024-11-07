package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplierTblV2;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.dao.v3.ApplierGroupTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.ApplierTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.MessengerTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.MhaDbReplicationTblDao;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.param.v2.resource.*;
import com.ctrip.framework.drc.console.service.impl.DalServiceImpl;
import com.ctrip.framework.drc.console.service.impl.inquirer.ApplierInquirer;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.service.v2.resource.impl.ResourceServiceImpl;
import com.ctrip.framework.drc.console.vo.v2.*;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.http.PageReq;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierInfoDto;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.console.service.v2.PojoBuilder.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

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
    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private MessengerGroupTblDao messengerGroupTblDao;
    @Mock
    private ApplierGroupTblV3Dao dbApplierGroupTblDao;
    @Mock
    private MhaDbReplicationTblDao mhaDbReplicationTblDao;
    @Mock
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Mock
    private DbTblDao dbTblDao;
    @Mock
    private ApplierTblV3Dao dbApplierTblDao;
    @Mock
    private MessengerTblV3Dao dbMessengerTblDao;
    @Mock
    private MysqlServiceV2 mysqlServiceV2;
    @Mock
    private MetaInfoServiceV2 metaInfoService;
    @Mock
    private DalServiceImpl dalService;
    @Mock
    private MetaProviderV2 metaProviderV2;
    @Mock
    private ResourceService resourceService2;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        Mockito.when(consoleConfig.getReplicatorMaxSize()).thenReturn(20L);
        Mockito.when(consoleConfig.getCenterRegion()).thenReturn("sha");
    }

    @Test
    public void testConfigureResource() throws Exception {
        Mockito.when(dcTblDao.queryByDcName(anyString())).thenReturn(getDcTbls().get(0));
        Mockito.when(resourceTblDao.queryByIp(anyString())).thenReturn(null);
        Mockito.when(resourceTblDao.insert(any(ResourceTbl.class))).thenReturn(0);

        ResourceBuildParam param = new ResourceBuildParam();
        param.setAz("AZ");
        param.setIp("127.0.0.1");
        param.setDcName("dc");
        param.setType("R");
        param.setTag("tag");
        resourceService.configureResource(param);
        Mockito.verify(resourceTblDao, Mockito.times(1)).insert(any(ResourceTbl.class));
        Mockito.verify(resourceTblDao, Mockito.never()).update(any(ResourceTbl.class));
    }

    @Test
    public void testBatchConfigureResource() throws Exception {
        Mockito.when(dcTblDao.queryByDcName(anyString())).thenReturn(getDcTbls().get(0));
        Mockito.when(resourceTblDao.queryByIps(Mockito.anyList())).thenReturn(new ArrayList<>());
        Mockito.when(resourceTblDao.insert(any(ResourceTbl.class))).thenReturn(0);

        ResourceBuildParam param = new ResourceBuildParam();
        param.setAz("AZ");
        param.setIps(Lists.newArrayList("127.0.0.1"));
        param.setDcName("dc");
        param.setType("R");
        param.setTag("tag");
        resourceService.batchConfigureResource(param);
        Mockito.verify(resourceTblDao, Mockito.never()).update(any(ResourceTbl.class));
    }

    @Test
    public void testOfflineResource() throws Exception {
        Mockito.when(resourceTblDao.queryByPk(Mockito.anyLong())).thenReturn(getResourceTbls().get(0));
        Mockito.when(replicatorTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        Mockito.when(applierTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        Mockito.when(resourceTblDao.update(any(ResourceTbl.class))).thenReturn(0);

        resourceService.offlineResource(1L);
        Mockito.verify(resourceTblDao, Mockito.times(1)).update(any(ResourceTbl.class));
    }

    @Test(expected = ConsoleException.class)
    public void testOfflineResource01() throws Exception {
        Mockito.when(resourceTblDao.queryByPk(Mockito.anyLong())).thenReturn(getResourceTbls().get(0));
        Mockito.when(replicatorTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(Lists.newArrayList(new ReplicatorTbl()));
        Mockito.when(resourceTblDao.update(any(ResourceTbl.class))).thenReturn(0);

        resourceService.offlineResource(1L);
    }

    @Test(expected = ConsoleException.class)
    public void testOfflineResource02() throws Exception {
        ResourceTbl resourceTbl = new ResourceTbl();
        resourceTbl.setType(1);
        Mockito.when(resourceTblDao.queryByPk(Mockito.anyLong())).thenReturn(resourceTbl);
        Mockito.when(resourceTblDao.update(any(ResourceTbl.class))).thenReturn(0);
        Mockito.when(applierTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(Lists.newArrayList(new ApplierTblV2()));

        resourceService.offlineResource(1L);
    }

    @Test
    public void testOnlineResource() throws Exception {
        Mockito.when(resourceTblDao.queryByPk(Mockito.anyLong())).thenReturn(getResourceTbls().get(0));
        Mockito.when(resourceTblDao.update(any(ResourceTbl.class))).thenReturn(0);
        resourceService.onlineResource(1L);

        Mockito.verify(resourceTblDao, Mockito.times(1)).update(any(ResourceTbl.class));
    }

    @Test
    public void testDeactivateResource() throws Exception {
        Mockito.when(resourceTblDao.queryByPk(Mockito.anyLong())).thenReturn(getResourceTbls().get(0));
        Mockito.when(resourceTblDao.update(any(ResourceTbl.class))).thenReturn(0);
        resourceService.deactivateResource(1L);

        Mockito.verify(resourceTblDao, Mockito.times(1)).update(any(ResourceTbl.class));
    }

    @Test
    public void testRecoverResource() throws Exception {
        Mockito.when(resourceTblDao.queryByPk(Mockito.anyLong())).thenReturn(getResourceTbls().get(0));
        Mockito.when(resourceTblDao.update(any(ResourceTbl.class))).thenReturn(0);
        resourceService.recoverResource(1L);

        Mockito.verify(resourceTblDao, Mockito.times(1)).update(any(ResourceTbl.class));
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
        Mockito.when(dcTblDao.queryByRegionName(anyString())).thenReturn(getDcTbls());
        Mockito.when(resourceTblDao.queryByDcAndTag(Mockito.anyList(), anyString(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(getResourceTbls());
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
        Mockito.when(dcTblDao.queryByRegionName(anyString())).thenReturn(getDcTbls());
        Mockito.when(resourceTblDao.queryByDcAndTag(Mockito.anyList(), anyString(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(getResourceTbls());
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
        Mockito.when(dcTblDao.queryByRegionName(anyString())).thenReturn(getDcTbls());
        Mockito.when(resourceTblDao.queryByDcAndTag(Mockito.anyList(), anyString(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(getResourceTbls());
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
    public void testQueryMhaByReplicator() throws Exception {
        Mockito.when(replicatorTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(getReplicatorTbls());
        Mockito.when(replicatorGroupTblDao.queryByIds(Mockito.anyList())).thenReturn(getReplicatorGroupTbls());
        Mockito.when(mhaTblV2Dao.queryByIds(Mockito.anyList())).thenReturn(getMhaTblV2s());

        List<MhaView> result = resourceService.queryMhaByReplicator(1L);
        Assert.assertEquals(result.size(), getMhaTblV2s().size());
    }

    @Test
    public void testQueryMhaByMessenger() throws Exception {
        Mockito.when(messengerTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(Lists.newArrayList(getMessenger()));
        Mockito.when(messengerGroupTblDao.queryByIds(Mockito.anyList())).thenReturn(Lists.newArrayList(getMessengerGroup()));
        Mockito.when(mhaTblV2Dao.queryByIds(Mockito.anyList())).thenReturn(Lists.newArrayList(getMhaTblV2()));

        List<ApplierReplicationView> result = resourceService.queryMhaByMessenger(1L);
        Assert.assertEquals(result.size(), 1);
    }

    @Test
    public void testQueryMhaReplicationByApplier() throws Exception {
        Mockito.when(applierTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(Lists.newArrayList(getApplierTblV2s().get(0)));
        Mockito.when(applierGroupTblDao.queryByIds(Mockito.anyList())).thenReturn(getApplierGroupTblV2s());
        Mockito.when(mhaTblV2Dao.queryByIds(Mockito.anyList())).thenReturn(getMhaTblV2s());
        Mockito.when(mhaReplicationTblDao.queryByIds(Mockito.anyList())).thenReturn(getMhaReplicationTbls());
        Mockito.when(dcTblDao.queryAllExist()).thenReturn(getDcTbls());

        List<ApplierReplicationView> result = resourceService.queryMhaReplicationByApplier(1L);
        Assert.assertEquals(result.size(), getMhaReplicationTbls().size());
    }

    @Test
    public void testQueryMhaDbReplicationByApplier() throws Exception {
        Mockito.when(dbApplierTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(Lists.newArrayList(getApplierTblV3s().get(0)));
        Mockito.when(dbApplierGroupTblDao.queryByIds(Mockito.anyList())).thenReturn(getApplierGroupTblV3s());
        Mockito.when(mhaDbReplicationTblDao.queryByIds(Mockito.anyList())).thenReturn(getMhaDbReplicationTbls());
        Mockito.when(mhaDbMappingTblDao.queryByIds(Mockito.anyList())).thenReturn(getMhaDbMappingTbls1());
        Mockito.when(mhaTblV2Dao.queryByIds(Mockito.anyList())).thenReturn(getMhaTblV2s());
        Mockito.when(dbTblDao.queryByIds(Mockito.anyList())).thenReturn(getDbTbls());
        Mockito.when(dcTblDao.queryAllExist()).thenReturn(getDcTbls());

        List<ApplierReplicationView> result = resourceService.queryMhaDbReplicationByApplier(1L);
        Assert.assertEquals(result.size(), 1);
    }

    @Test
    public void testMigrateReplicator() throws Exception {
        List<ResourceTbl> resourceTbls = getReplicatorResources();
        Mockito.when(resourceTblDao.queryByIp(Mockito.eq("ip1"), Mockito.anyInt())).thenReturn(resourceTbls.get(0));
        Mockito.when(resourceTblDao.queryByIp(Mockito.eq("ip2"), Mockito.anyInt())).thenReturn(resourceTbls.get(1));
        Mockito.when(replicatorTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(PojoBuilder.getReplicatorTbls());
        Mockito.when(replicatorTblDao.queryByRGroupIds(Mockito.anyList(), Mockito.anyInt())).thenReturn(PojoBuilder.getReplicatorTbls());

        Mockito.when(replicatorGroupTblDao.queryById(Mockito.anyLong())).thenReturn(PojoBuilder.getReplicatorGroupTbls().get(0));
        Mockito.when(mhaTblV2Dao.queryById(Mockito.anyLong())).thenReturn(PojoBuilder.getMhaTblV2());
        Mockito.when(mysqlServiceV2.getMhaExecutedGtid(anyString())).thenReturn("gtid");
        Mockito.when(metaInfoService.findAvailableApplierPort(anyString())).thenReturn(2020);
        Mockito.when(replicatorTblDao.update(any(ReplicatorTbl.class))).thenReturn(1);
        int result = resourceService.migrateResource("ip2", "ip1", 0);
        Assert.assertEquals(result, PojoBuilder.getReplicatorTbls().size());
    }

    @Test
    public void testPartialMigrateReplicator() throws Exception {
        List<ResourceTbl> resourceTbls = getReplicatorResources();
        Mockito.when(resourceTblDao.queryByIp(Mockito.eq("ip1"), Mockito.anyInt())).thenReturn(resourceTbls.get(0));
        Mockito.when(resourceTblDao.queryByIp(Mockito.eq("ip2"), Mockito.anyInt())).thenReturn(resourceTbls.get(1));
        Mockito.when(replicatorTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(PojoBuilder.getReplicatorTbls());
        Mockito.when(replicatorTblDao.queryByRGroupIds(Mockito.anyList(), Mockito.anyInt())).thenReturn(PojoBuilder.getReplicatorTbls());

        Mockito.when(replicatorGroupTblDao.queryById(Mockito.anyLong())).thenReturn(PojoBuilder.getReplicatorGroupTbls().get(0));
        Mockito.when(mhaTblV2Dao.queryById(Mockito.anyLong())).thenReturn(PojoBuilder.getMhaTblV2());
        Mockito.when(mysqlServiceV2.getMhaExecutedGtid(anyString())).thenReturn("gtid");
        Mockito.when(metaInfoService.findAvailableApplierPort(anyString())).thenReturn(2020);
        Mockito.when(replicatorTblDao.update(any(ReplicatorTbl.class))).thenReturn(1);
        Mockito.when(mhaTblV2Dao.queryByMhaNames(Mockito.anyList(), Mockito.anyInt())).thenReturn(Lists.newArrayList(PojoBuilder.getMhaTblV2()));
        Mockito.when(replicatorGroupTblDao.queryByMhaIds(Mockito.anyList(), Mockito.anyInt())).thenReturn(PojoBuilder.getReplicatorGroupTbls());
        int result = resourceService.partialMigrateReplicator(new ReplicatorMigrateParam("ip1", "ip2", Lists.newArrayList("mha")));
        Assert.assertEquals(result, PojoBuilder.getReplicatorTbls().size());
    }

    @Test
    public void testMigrateSlaveReplicator() throws Exception {
        List<ResourceTbl> resourceTbls = getReplicatorResources();
        List<ReplicatorTbl> replicatorTbls = getReplicatorTbls();
        replicatorTbls.stream().filter(e -> e.getId().equals(200L)).forEach(e -> e.setMaster(0));

        Mockito.when(resourceTblDao.queryByIp(Mockito.eq("ip1"), Mockito.anyInt())).thenReturn(resourceTbls.get(0));
        Mockito.when(resourceTblDao.queryByIp(Mockito.eq("ip2"), Mockito.anyInt())).thenReturn(resourceTbls.get(1));
        Mockito.when(replicatorTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(replicatorTbls);
        Mockito.when(replicatorTblDao.queryByRGroupIds(Mockito.anyList(), Mockito.anyInt())).thenReturn(PojoBuilder.getReplicatorTbls());

        Mockito.when(replicatorGroupTblDao.queryById(Mockito.anyLong())).thenReturn(PojoBuilder.getReplicatorGroupTbls().get(0));
        Mockito.when(mhaTblV2Dao.queryById(Mockito.anyLong())).thenReturn(PojoBuilder.getMhaTblV2());
        Mockito.when(mysqlServiceV2.getMhaExecutedGtid(anyString())).thenReturn("gtid");
        Mockito.when(metaInfoService.findAvailableApplierPort(anyString())).thenReturn(2020);
        Mockito.when(replicatorTblDao.update(any(ReplicatorTbl.class))).thenReturn(1);
        int result = resourceService.migrateSlaveReplicator("ip2", "ip1");
        Assert.assertEquals(result, 1);
    }

    @Test
    public void testMigrateApplier() throws Exception {
        List<ResourceTbl> resourceTbls = getApplierResources();
        Mockito.when(resourceTblDao.queryByIp(Mockito.eq("ip1"), Mockito.anyInt())).thenReturn(resourceTbls.get(0));
        Mockito.when(resourceTblDao.queryByIp(Mockito.eq("ip2"), Mockito.anyInt())).thenReturn(resourceTbls.get(1));
        Mockito.when(applierTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(PojoBuilder.getApplierTblV2s());
        Mockito.when(dbApplierTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(PojoBuilder.getApplierTblV3s());
        Mockito.when(messengerTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(Lists.newArrayList(PojoBuilder.getMessenger()));
        Mockito.when(dbMessengerTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(new ArrayList<>());

        Mockito.when(applierTblDao.queryByApplierGroupIds(Mockito.anyList(), Mockito.anyInt())).thenReturn(PojoBuilder.getApplierTblV2s());
        Mockito.when(dbApplierTblDao.queryByApplierGroupIds(Mockito.anyList(), Mockito.anyInt())).thenReturn(PojoBuilder.getApplierTblV3s());
        Mockito.when(messengerTblDao.queryByGroupIds(Mockito.anyList())).thenReturn(Lists.newArrayList(PojoBuilder.getMessenger()));
        Mockito.when(dbMessengerTblDao.queryByGroupIds(Mockito.anyList())).thenReturn(new ArrayList<>());

        Mockito.when(applierTblDao.update(Mockito.anyList())).thenReturn(new int[1]);
        Mockito.when(dbApplierTblDao.update(Mockito.anyList())).thenReturn(new int[1]);
        Mockito.when(messengerTblDao.update(Mockito.anyList())).thenReturn(new int[1]);
        Mockito.when(dbMessengerTblDao.update(Mockito.anyList())).thenReturn(new int[1]);

        int result = resourceService.migrateResource("ip2", "ip1", 1);
        Assert.assertEquals(result, 5);
    }

    @Test
    public void testCheckResourceAz() throws Exception {
        Mockito.when(resourceTblDao.queryAllExist()).thenReturn(PojoBuilder.getResourceTbls());
        Mockito.when(dbApplierTblDao.queryAllExist()).thenReturn(PojoBuilder.getApplierTblV3s());
        Mockito.when(dbTblDao.queryById(Mockito.anyLong())).thenReturn(PojoBuilder.getDbTbls().get(0));
        Mockito.when(applierTblDao.queryAllExist()).thenReturn(PojoBuilder.getApplierTblV2s());
        Mockito.when(mhaTblV2Dao.queryById(Mockito.anyLong())).thenReturn(PojoBuilder.getMhaTblV2());
        Mockito.when(messengerTblDao.queryAllExist()).thenReturn(Lists.newArrayList(PojoBuilder.getMessenger()));
        Mockito.when(replicatorTblDao.queryAllExist()).thenReturn(PojoBuilder.getReplicatorTbls());
        Mockito.when(mhaTblV2Dao.queryAll()).thenReturn(PojoBuilder.getMhaTblV2s());
        Mockito.when(applierGroupTblDao.queryAll()).thenReturn(PojoBuilder.getApplierGroupTblV2s());
        Mockito.when(mhaReplicationTblDao.queryAll()).thenReturn(PojoBuilder.getMhaReplicationTbls());
        Mockito.when(replicatorGroupTblDao.queryAll()).thenReturn(PojoBuilder.getReplicatorGroupTbls());
        Mockito.when(messengerGroupTblDao.queryAll()).thenReturn(PojoBuilder.getMessengerGroups());
        Mockito.when(dbApplierGroupTblDao.queryAll()).thenReturn(PojoBuilder.getApplierGroupTblV3s());
        Mockito.when(mhaDbReplicationTblDao.queryAll()).thenReturn(PojoBuilder.getMhaDbReplicationTbls());
        Mockito.when(mhaDbMappingTblDao.queryAll()).thenReturn(PojoBuilder.getMhaDbMappingTbls2());
        Mockito.when(dbTblDao.queryAll()).thenReturn(PojoBuilder.getDbTbls());

        ResourceSameAzView result = resourceService.checkResourceAz();
        Assert.assertEquals(result.getApplierDbList().size(), 1);
        Assert.assertEquals(result.getApplierMhaReplicationList().size(), 1);
        Assert.assertEquals(result.getMessengerMhaList().size(), 1);
        Assert.assertEquals(result.getReplicatorMhaList().size(), 1);

    }

    @Test
    public void testGetResourceViewByIp() throws Exception {
        Mockito.when(resourceTblDao.queryByIp(anyString(), Mockito.anyInt())).thenReturn(getReplicatorResources().get(0));
        Mockito.when(resourceTblDao.queryByDcAndTag(Mockito.anyList(), anyString(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(getReplicatorResources());
        Mockito.when(replicatorTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(getReplicatorTbls());
        List<ResourceView> views = resourceService.getResourceViewByIp("ip1");
        Assert.assertEquals(views.size(), 1);

        Mockito.when(resourceTblDao.queryByIp(anyString(), Mockito.anyInt())).thenReturn(getApplierResources().get(0));
        Mockito.when(resourceTblDao.queryByDcAndTag(Mockito.anyList(), anyString(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(getApplierResources());
        Mockito.when(applierTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(getApplierTblV2s());
        Mockito.when(messengerTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(getMessengers());
        Mockito.when(dbMessengerTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        Mockito.when(dbApplierTblDao.queryByResourceIds(Mockito.anyList())).thenReturn(getApplierTblV3s());
        views = resourceService.getResourceViewByIp("ip1");
        Assert.assertEquals(views.size(), 1);
    }

    @Test
    public void testPartialMigrateApplier() throws Exception {
        List<ResourceTbl> resourceTbls = getApplierResources();
        ApplierMigrateParam param = new ApplierMigrateParam();
        param.setNewIp("newIp");
        param.setOldIp("oldIp");
        List<ApplierResourceDto> dtos = new ArrayList<>();
        param.setApplierResourceDtos(dtos);
        dtos.add(new ApplierResourceDto(200L, 1));
        dtos.add(new ApplierResourceDto(200L, 2));
        dtos.add(new ApplierResourceDto(200L, 3));
        dtos.add(new ApplierResourceDto(200L, 4));

        Mockito.when(resourceTblDao.queryByIp(Mockito.eq("newIp"), Mockito.anyInt())).thenReturn(resourceTbls.get(1));
        Mockito.when(resourceTblDao.queryByIp(Mockito.eq("oldIp"), Mockito.anyInt())).thenReturn(resourceTbls.get(0));

        Mockito.when(applierTblDao.queryByIds(Mockito.anyList())).thenReturn(getApplierTblV2s());
        Mockito.when(messengerTblDao.queryByIds(Mockito.anyList())).thenReturn(getMessengers());
        Mockito.when(dbMessengerTblDao.queryByIds(Mockito.anyList())).thenReturn(new ArrayList<>());
        Mockito.when(dbApplierTblDao.queryByIds(Mockito.anyList())).thenReturn(getApplierTblV3s());

        Mockito.when(applierTblDao.queryByApplierGroupIds(Mockito.anyList(), Mockito.anyInt())).thenReturn(PojoBuilder.getApplierTblV2s());
        Mockito.when(dbApplierTblDao.queryByApplierGroupIds(Mockito.anyList(), Mockito.anyInt())).thenReturn(PojoBuilder.getApplierTblV3s());
        Mockito.when(messengerTblDao.queryByGroupIds(Mockito.anyList())).thenReturn(Lists.newArrayList(PojoBuilder.getMessenger()));
        Mockito.when(dbMessengerTblDao.queryByGroupIds(Mockito.anyList())).thenReturn(new ArrayList<>());

        Mockito.when(applierTblDao.update(Mockito.anyList())).thenReturn(new int[1]);
        Mockito.when(dbApplierTblDao.update(Mockito.anyList())).thenReturn(new int[1]);
        Mockito.when(messengerTblDao.update(Mockito.anyList())).thenReturn(new int[1]);
        Mockito.when(dbMessengerTblDao.update(Mockito.anyList())).thenReturn(new int[1]);

        int result = resourceService.partialMigrateApplier(param);
        Assert.assertEquals(result, 6);
    }

    @Test
    public void testHandOffMultiple() {
        List<ResourceView> availableResource = new ArrayList<>();
        ResourceView r1 = buildResourceView("10.10", "az1", 8L);
        ResourceView r2 = buildResourceView("10.11", "az1", 10L);
        availableResource.add(r1);
        availableResource.add(r2);
        Collections.sort(availableResource);

        List<ResourceView> resourceViews = resourceService.handOffResource(Lists.newArrayList("10.10"), availableResource);
        Assert.assertEquals(1, resourceViews.size());
        Assert.assertSame(r2, resourceViews.get(0));
        Assert.assertEquals(11L, r2.getInstanceNum().longValue());
        Assert.assertEquals(7, r1.getInstanceNum().longValue());

        resourceViews = resourceService.handOffResource(Lists.newArrayList(), availableResource);
        Assert.assertEquals(1, resourceViews.size());
        Assert.assertSame(r1, resourceViews.get(0));
        Assert.assertEquals(11L, r2.getInstanceNum().longValue());
        Assert.assertEquals(8, r1.getInstanceNum().longValue());

        resourceViews = resourceService.handOffResource(Lists.newArrayList("10.11"), availableResource);
        Assert.assertEquals(1, resourceViews.size());
        Assert.assertSame(r1, resourceViews.get(0));
        Assert.assertEquals(10L, r2.getInstanceNum().longValue());
        Assert.assertEquals(9, r1.getInstanceNum().longValue());

        resourceViews = resourceService.handOffResource(Lists.newArrayList(), availableResource);
        Assert.assertEquals(1, resourceViews.size());
        Assert.assertSame(r1, resourceViews.get(0));
        Assert.assertEquals(10L, r2.getInstanceNum().longValue());
        Assert.assertEquals(10L, r1.getInstanceNum().longValue());

        resourceService.handOffResource(Lists.newArrayList(), availableResource);
        resourceService.handOffResource(Lists.newArrayList(), availableResource);
        Assert.assertEquals(11L, r2.getInstanceNum().longValue());
        Assert.assertEquals(11L, r1.getInstanceNum().longValue());

        resourceService.handOffResource(Lists.newArrayList(r1.getIp(),r2.getIp()), availableResource);
        Assert.assertEquals(10L, r2.getInstanceNum().longValue());
        Assert.assertEquals(10L, r1.getInstanceNum().longValue());


    }

    private static ResourceView buildResourceView(String ip, String az1, long instanceNum) {
        ResourceView resourceView = new ResourceView();
        resourceView.setIp(ip);
        resourceView.setAz(az1);
        resourceView.setInstanceNum(instanceNum);
        return resourceView;
    }

    @Test
    public void testGetAllInstanceAzInfo() throws Exception {
        Mockito.when(dalService.getMhaList(any())).thenReturn(PojoBuilder.getMhaInstanceGroups());
        Mockito.when(metaProviderV2.getDrc()).thenReturn(PojoBuilder.getDrc2());
        Mockito.when(resourceTblDao.queryAllExist()).thenReturn(PojoBuilder.getResourceTbls());
        Mockito.when(dcTblDao.queryAllExist()).thenReturn(PojoBuilder.getDcTbls());
        Mockito.when(mhaTblV2Dao.queryAllExist()).thenReturn(PojoBuilder.getMhaTblV2s());
        Map<String, Set<String>> regions2DcMap = Maps.newHashMap();
        regions2DcMap.put("sha", Sets.newHashSet());
        Mockito.when(consoleConfig.getRegion2dcsMapping()).thenReturn(regions2DcMap);

        MhaAzView result = resourceService.getAllInstanceAzInfo();
        Assert.assertEquals(result.getAz2mhaName().size(), 1);
        Assert.assertEquals(result.getAz2DbInstance().size(), 1);
        Assert.assertEquals(result.getAz2DbInstance().get("AZ").size(), 2);
        Assert.assertEquals(result.getAz2ApplierInstance().size(), 1);
        Assert.assertEquals(result.getAz2ApplierInstance().get("shaxy").size(), 0);
        Assert.assertEquals(result.getAz2ReplicatorInstance().size(), 2);
        Assert.assertEquals(result.getAz2DrcDb().size(), 1);
    }

    @Mock
    ApplierInquirer inquirer;


    @Test
    public void testGetAppliersInAz() throws Exception {
        String testRegion = "sha";
        Mockito.when(metaProviderV2.getDrc()).thenReturn(PojoBuilder.getDrc2());
        MockedStatic<ApplierInquirer> mockedStatic = Mockito.mockStatic(ApplierInquirer.class);
        mockedStatic.when(ApplierInquirer::getInstance).thenReturn(inquirer);
        Future<List<ApplierInfoDto>> mockFuture = Mockito.mock(Future.class);
        Mockito.when(mockFuture.get(Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenReturn(PojoBuilder.getApplierInfoDtos());
        Mockito.when(inquirer.query(Mockito.anyString())).thenReturn(mockFuture);

        List<ApplierInfoDto> result = resourceService.getAppliersInAz(testRegion, Lists.newArrayList("ip"));
        Assert.assertEquals(result.size(), 1);
    }

    @Test
    public void testMhaRelatedDrcDb() {
        Map<String, Set<String>> az2MhaName = Maps.newHashMap();
        Set<String> rbmhas = Sets.newHashSet("mha1");
        az2MhaName.put("dcId",rbmhas);

        Drc drc = new Drc();

        Map<String, Set<String>> result = resourceService.mhaRelatedDrcDb(az2MhaName,drc);
        Assert.assertEquals(result.size(),1);
        Assert.assertEquals(result.get("dcId").size(), 0);


        Dc dc = new Dc();
        dc.setId("dcId");
        drc.addDc(dc);
        result = resourceService.mhaRelatedDrcDb(az2MhaName,drc);
        Assert.assertEquals(result.size(),1);
        Assert.assertEquals(result.get("dcId").size(), 0);

        DbCluster dbCluster = new DbCluster();
        dc.addDbCluster(dbCluster);
        Assert.assertEquals(result.size(),1);
        Assert.assertEquals(result.get("dcId").size(), 0);

        Applier applier = new Applier();
        applier.setTargetMhaName("mha1");
        applier.setIncludedDbs("includeDb");
        dbCluster.addApplier(applier);

        result = resourceService.mhaRelatedDrcDb(az2MhaName,drc);
        Assert.assertEquals(result.size(),1);
        Assert.assertEquals(result.get("dcId").size(), 1);

        dbCluster.setMhaName("mha1");
        applier.setTargetMhaName("mha2");
        result = resourceService.mhaRelatedDrcDb(az2MhaName,drc);
        Assert.assertEquals(result.size(),1);
        Assert.assertEquals(result.get("dcId").size(), 1);

        Applier applier2 = new Applier();
        applier2.setParent(dbCluster);
        applier2.setIncludedDbs("includeDb2");
        dbCluster.addApplier(applier2);
        result = resourceService.mhaRelatedDrcDb(az2MhaName,drc);
        Assert.assertEquals(result.size(),1);
        Assert.assertEquals(result.get("dcId").size(), 2);

        Applier applier3 = new Applier();
        applier3.setNameFilter("fncaggregatemerchantcontractshard00db\\.(?!qmq_msg_queue).*,fncaggregatemerchantcontractshard01db\\.(?!qmq_msg_queue).*");
        dbCluster.addApplier(applier3);
        result = resourceService.mhaRelatedDrcDb(az2MhaName,drc);
        Assert.assertEquals(result.size(),1);
        Assert.assertEquals(result.get("dcId").size(), 4);

        applier3.setNameFilter("");
        result = resourceService.mhaRelatedDrcDb(az2MhaName,drc);
        Assert.assertEquals(result.size(),1);
        Assert.assertEquals(result.get("dcId").size(), 2);

    }
}
