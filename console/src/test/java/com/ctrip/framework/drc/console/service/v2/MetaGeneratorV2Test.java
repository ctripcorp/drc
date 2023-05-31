package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.service.v2.impl.MetaGeneratorV2;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/5/31 10:17
 */
public class MetaGeneratorV2Test {
    @InjectMocks
    private MetaGeneratorV2 metaGenerator;
    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private MessengerServiceV2 messengerService;
    @Mock
    private DataMediaServiceV2 dataMediaService;
    @Mock
    private ApplierGroupTblV2Dao applierGroupTblDao;
    @Mock
    private ApplierTblV2Dao applierTblDao;
    @Mock
    private DbReplicationTblDao dbReplicationTblDao;
    @Mock
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Mock
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Mock
    private MhaTblV2Dao mhaTblDao;
    @Mock
    private DbTblDao dbTblDao;
    @Mock
    private BuTblDao buTblDao;
    @Mock
    private RouteTblDao routeTblDao;
    @Mock
    private ProxyTblDao proxyTblDao;
    @Mock
    private DcTblDao dcTblDao;
    @Mock
    private ResourceTblDao resourceTblDao;
    @Mock
    private MachineTblDao machineTblDao;
    @Mock
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Mock
    private ClusterManagerTblDao clusterManagerTblDao;
    @Mock
    private ZookeeperTblDao zookeeperTblDao;
    @Mock
    private ReplicatorTblDao replicatorTblDao;

    @Before
    public void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        Mockito.when(consoleConfig.getPublicCloudRegion()).thenReturn(new HashSet<>());
        Mockito.when(consoleConfig.getRegion()).thenReturn("");

        Mockito.when(buTblDao.queryAll()).thenReturn(getButbls());
        Mockito.when(routeTblDao.queryAll()).thenReturn(getRouteTbls());
        Mockito.when(proxyTblDao.queryAll()).thenReturn(getProxyTbls());
        Mockito.when(dcTblDao.queryAll()).thenReturn(getDcTbls());
        Mockito.when(mhaTblDao.queryAll()).thenReturn(getMhaTbls());
        Mockito.when(resourceTblDao.queryAll()).thenReturn(getResourceTbls());
        Mockito.when(machineTblDao.queryAll()).thenReturn(getMachineTbls());
        Mockito.when(replicatorGroupTblDao.queryAll()).thenReturn(getReplicatorGroupTbls());
        Mockito.when(applierGroupTblDao.queryAll()).thenReturn(getApplierGroupTbls());
        Mockito.when(clusterManagerTblDao.queryAll()).thenReturn(getClusterManagerTbls());
        Mockito.when(zookeeperTblDao.queryAll()).thenReturn(getZookeeperTbls());
        Mockito.when(replicatorTblDao.queryAll()).thenReturn(getReplicatorTbls());
        Mockito.when(applierTblDao.queryAll()).thenReturn(getApplierTbls());
        Mockito.when(mhaReplicationTblDao.queryAll()).thenReturn(getMhaReplicationTbls());
        Mockito.when(mhaDbMappingTblDao.queryAll()).thenReturn(getMhaDbMappingTbls());
        Mockito.when(dbReplicationTblDao.queryAll()).thenReturn(getDbReplicationTbls());
        Mockito.when(dbTblDao.queryAll()).thenReturn(getDbTbls());

        Mockito.when(messengerService.generateMessengers(Mockito.anyLong())).thenReturn(getMessengers());
        Mockito.when(dataMediaService.generateConfig(Mockito.any())).thenReturn(getDataMediaConfig());
    }

    @Test
    public void testGetDrc() throws Exception {
        Drc drc = metaGenerator.getDrc();
        Dc dc = drc.findDc("dc");
        List<Route> routes = dc.getRoutes();
        Assert.assertEquals(routes.size(), 1);
    }

    private DataMediaConfig getDataMediaConfig() {
        DataMediaConfig dataMediaConfig = new DataMediaConfig();

        RowsFilterConfig rowsFilterConfig = new RowsFilterConfig();
        rowsFilterConfig.setMode("mode");
        rowsFilterConfig.setRegistryKey("");
        rowsFilterConfig.setTables("rowTable");
        String configJson = "{\"parameterList\":[{\"columns\":[\"AgentUID\"],\"illegalArgument\":false,\"context\":\"SIN\",\"fetchMode\":0,\"userFilterMode\":\"uid\"}],\"drcStrategyId\":0,\"routeStrategyId\":0}";
        rowsFilterConfig.setConfigs(JsonUtils.fromJson(configJson, RowsFilterConfig.Configs.class));

        ColumnsFilterConfig columnsFilterConfig = new ColumnsFilterConfig();
        columnsFilterConfig.setTables("columnTable");
        columnsFilterConfig.setMode("Mode");
        columnsFilterConfig.setColumns(Lists.newArrayList("column"));

        dataMediaConfig.setRowsFilters(Lists.newArrayList(rowsFilterConfig));
        dataMediaConfig.setColumnsFilters(Lists.newArrayList(columnsFilterConfig));
        return dataMediaConfig;
    }

    private List<Messenger> getMessengers() {
        Messenger messenger = new Messenger();
        messenger.setIp("127.0.0.1");
        messenger.setPort(30);
        messenger.setGtidExecuted("messengerGtId");
        messenger.setNameFilter("nameFilter");
        return Lists.newArrayList(messenger);
    }

    private List<DcTbl> getDcTbls() {
        DcTbl dcTbl = new DcTbl();
        dcTbl.setId(1L);
        dcTbl.setDeleted(BooleanEnum.FALSE.getCode());
        dcTbl.setDcName("dc");
        dcTbl.setRegionName("region");
        return Lists.newArrayList(dcTbl);
    }

    private List<RouteTbl> getRouteTbls() {
        RouteTbl routeTbl = new RouteTbl();
        routeTbl.setDeleted(0);
        routeTbl.setSrcDcId(1L);
        routeTbl.setDstDcId(1L);
        routeTbl.setId(1L);
        routeTbl.setRouteOrgId(1L);
        routeTbl.setSrcProxyIds("0");
        routeTbl.setOptionalProxyIds("1");
        routeTbl.setDstProxyIds("2");
        routeTbl.setTag("console");

        return Lists.newArrayList(routeTbl);
    }

    private List<ProxyTbl> getProxyTbls() {
        List<ProxyTbl> proxyTbls = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            ProxyTbl proxyTbl = new ProxyTbl();
            proxyTbl.setDeleted(0);
            proxyTbl.setId(Long.valueOf(i));
            proxyTbl.setUri("uri");
            proxyTbls.add(proxyTbl);
        }
        return proxyTbls;
    }

    private List<ResourceTbl> getResourceTbls() {
        List<ResourceTbl> resourceTbls = new ArrayList<>();
        for (ModuleEnum value : ModuleEnum.values()) {
            ResourceTbl resourceTbl = new ResourceTbl();
            resourceTbl.setDeleted(0);
            resourceTbl.setId(1L);
            resourceTbl.setIp("127.0.0.1");
            resourceTbls.add(resourceTbl);
            resourceTbl.setType(value.getCode());
            resourceTbl.setDcId(1L);
        }
        return resourceTbls;
    }

    private List<ClusterManagerTbl> getClusterManagerTbls() {
        ClusterManagerTbl clusterManagerTbl = new ClusterManagerTbl();
        clusterManagerTbl.setDeleted(0);
        clusterManagerTbl.setPort(80);
        clusterManagerTbl.setMaster(1);
        clusterManagerTbl.setResourceId(1L);
        return Lists.newArrayList(clusterManagerTbl);
    }

    private List<ZookeeperTbl> getZookeeperTbls() {
        ZookeeperTbl zookeeperTbl = new ZookeeperTbl();
        zookeeperTbl.setDeleted(0);
        zookeeperTbl.setPort(80);
        zookeeperTbl.setResourceId(1L);
        return Lists.newArrayList(zookeeperTbl);
    }

    private List<MhaTblV2> getMhaTbls() {
        MhaTblV2 mhaTbl = new MhaTblV2();
        mhaTbl.setDeleted(0);
        mhaTbl.setMhaName("mhaA");
        mhaTbl.setId(1L);
        mhaTbl.setDcId(1L);
        mhaTbl.setBuId(1L);
        mhaTbl.setClusterName("cluster");
        mhaTbl.setReadUser("readUser");
        mhaTbl.setReadPassword("readPassword");
        mhaTbl.setWriteUser("writeUser");
        mhaTbl.setWritePassword("writePassword");
        mhaTbl.setMonitorUser("monitorUser");
        mhaTbl.setMonitorPassword("monitorPassword");
        mhaTbl.setApplyMode(0);

        return Lists.newArrayList(mhaTbl);
    }

    private List<BuTbl> getButbls() {
        BuTbl buTbl = new BuTbl();
        buTbl.setDeleted(0);
        buTbl.setBuName("BU");
        buTbl.setId(1L);
        return Lists.newArrayList(buTbl);
    }

    private List<MachineTbl> getMachineTbls() {
        MachineTbl machineTbl = new MachineTbl();
        machineTbl.setDeleted(0);
        machineTbl.setMhaId(1L);
        machineTbl.setMaster(1);
        machineTbl.setUuid("uuid");
        machineTbl.setIp("127.0.0.1");
        machineTbl.setPort(3306);
        return Lists.newArrayList(machineTbl);
    }

    private List<ReplicatorGroupTbl> getReplicatorGroupTbls() {
        ReplicatorGroupTbl replicatorGroupTbl = new ReplicatorGroupTbl();
        replicatorGroupTbl.setDeleted(0);
        replicatorGroupTbl.setId(1L);
        replicatorGroupTbl.setMhaId(1L);
        return Lists.newArrayList(replicatorGroupTbl);
    }

    private List<ReplicatorTbl> getReplicatorTbls() {
        ReplicatorTbl replicatorTbl = new ReplicatorTbl();
        replicatorTbl.setDeleted(0);
        replicatorTbl.setRelicatorGroupId(1L);
        replicatorTbl.setResourceId(1L);
        replicatorTbl.setApplierPort(1010);
        replicatorTbl.setGtidInit("gtId");
        replicatorTbl.setPort(3030);
        return Lists.newArrayList(replicatorTbl);
    }

    private List<MhaReplicationTbl> getMhaReplicationTbls(){
        MhaReplicationTbl mhaReplicationTbl = new MhaReplicationTbl();
        mhaReplicationTbl.setDeleted(0);
        mhaReplicationTbl.setId(1L);
        mhaReplicationTbl.setSrcMhaId(1L);
        mhaReplicationTbl.setDstMhaId(1L);
        return Lists.newArrayList(mhaReplicationTbl);
    }

    private List<ApplierGroupTblV2> getApplierGroupTbls() {
        ApplierGroupTblV2 applierGroupTbl = new ApplierGroupTblV2();
        applierGroupTbl.setDeleted(0);
        applierGroupTbl.setId(1L);
        applierGroupTbl.setGtidInit("applierGtId");
        applierGroupTbl.setMhaReplicationId(1L);
        return Lists.newArrayList(applierGroupTbl);
    }

    private List<MhaDbMappingTbl> getMhaDbMappingTbls() {
        MhaDbMappingTbl mhaDbMappingTbl = new MhaDbMappingTbl();
        mhaDbMappingTbl.setDeleted(0);
        mhaDbMappingTbl.setMhaId(1L);
        mhaDbMappingTbl.setDbId(1L);
        mhaDbMappingTbl.setId(1L);

        return Lists.newArrayList(mhaDbMappingTbl);
    }

    private List<DbTbl> getDbTbls() {
        DbTbl dbTbl = new DbTbl();
        dbTbl.setDeleted(0);
        dbTbl.setId(1L);
        dbTbl.setDbName("db");
        return Lists.newArrayList(dbTbl);
    }

    private List<DbReplicationTbl> getDbReplicationTbls() {
        DbReplicationTbl tbl = new DbReplicationTbl();
        tbl.setDeleted(0);
        tbl.setSrcMhaDbMappingId(1L);
        tbl.setDstMhaDbMappingId(1L);
        tbl.setSrcLogicTableName("srcTable");
        tbl.setDstLogicTableName("dstTable");
        tbl.setId(1L);
        return Lists.newArrayList(tbl);
    }

    private List<ApplierTblV2> getApplierTbls() {
        ApplierTblV2 tbl = new ApplierTblV2();
        tbl.setId(1L);
        tbl.setDeleted(0);
        tbl.setResourceId(1L);
        tbl.setMaster(1);
        tbl.setPort(2020);
        tbl.setApplierGroupId(1L);
        return Lists.newArrayList(tbl);
    }

}
