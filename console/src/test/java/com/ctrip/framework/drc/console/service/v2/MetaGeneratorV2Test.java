package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.service.v2.impl.MetaGeneratorV2;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;

import static com.ctrip.framework.drc.console.service.v2.MetaGeneratorBuilder.*;

/**
 * Created by dengquanliang
 * 2023/5/31 10:17
 */
public class MetaGeneratorV2Test {
    private static Logger logger = LoggerFactory.getLogger(MetaGeneratorV2Test.class);

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

        logger.info("drc: \n{}", drc.toString());
        Assert.assertEquals(routes.size(), 1);
    }

    private List<Messenger> getMessengers() {
        Messenger messenger = new Messenger();
        messenger.setIp("127.0.0.1");
        messenger.setPort(30);
        messenger.setGtidExecuted("messengerGtId");
        messenger.setNameFilter("nameFilter");
        return Lists.newArrayList(messenger);
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


}
