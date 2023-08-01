package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.AbstractTest;
import com.ctrip.framework.drc.console.dao.entity.DataConsistencyMonitorTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.pojo.MonitorMetaInfo;
import com.ctrip.framework.drc.console.service.monitor.MonitorService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.console.AllTests.DRC_XML;

public class MetaInfoServiceTwoImplTest extends AbstractTest {

    @InjectMocks
    private MetaInfoServiceTwoImpl metaInfoServiceTwo;

    @Mock
    private DbClusterSourceProvider dbClusterSourceProvider;

    @Mock
    private MetaInfoServiceImpl metaInfoService;

    @Mock
    private MonitorService monitorService;

    private DalUtils dalUtils = DalUtils.getInstance();

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        Mockito.when(monitorService.getMhaNamesToBeMonitored()).thenReturn(Lists.newArrayList("mha1dc1", "mha2dc1", "mha3dc1", "mha1dc2", "mha2dc2", "mha3dc2", "mha1dc3", "mha2dc3", "mha3dc3"));
    }

    @Test
    public void testGetResources() {
        List<String> resources = metaInfoServiceTwo.getResources("shaoy", "R");
        Assert.assertNotEquals(0, resources.size());

        resources = metaInfoServiceTwo.getResources("shaoy", "NOSUCHDES");
        Assert.assertEquals(0, resources.size());
    }

    @Test
    public void testGetMasterMySQLEndpoint() throws Exception {

        Drc drc = DefaultSaxParser.parse(DRC_XML);
        drc.getDcs().get("dc1").getDbClusters().get("dbcluster1.mha1dc1").getDbs().getDbs().clear();
        drc.getDcs().get("dc1").getDbClusters().get("dbcluster1.mha2dc1").getDbs().getDbs().forEach(m -> m.setMaster(false));
        Mockito.doReturn(drc).when(dbClusterSourceProvider).getDrc();
        MonitorMetaInfo monitorMetaInfo = metaInfoServiceTwo.getMonitorMetaInfo();
        Map<MetaKey, MySqlEndpoint> masterMySQLEndpoint = monitorMetaInfo.getMasterMySQLEndpoint();
        Assert.assertEquals(5, masterMySQLEndpoint.size());
        MetaKey metaKey = new MetaKey.Builder()
                .dc("dc1")
                .clusterId("dbcluster1.mha1dc1")
                .clusterName("dbcluster1")
                .mhaName("mha1dc1")
                .build();
        Assert.assertFalse(masterMySQLEndpoint.containsKey(metaKey));
        prettyPrintJSON("masterMySQLEndpoint", new JSONObject(masterMySQLEndpoint).toString());
    }

    @Test
    public void testGetSlaveMySQLEndpoint() throws Exception {

        Drc drc = DefaultSaxParser.parse(DRC_XML);
        drc.getDcs().get("dc1").getDbClusters().get("dbcluster1.mha1dc1").getDbs().getDbs().clear();
        drc.getDcs().get("dc1").getDbClusters().get("dbcluster1.mha2dc1").getDbs().getDbs().forEach(m -> m.setMaster(false));
        Mockito.doReturn(drc).when(dbClusterSourceProvider).getDrc();
        MonitorMetaInfo monitorMetaInfo = metaInfoServiceTwo.getMonitorMetaInfo();
        Map<MetaKey, MySqlEndpoint> slaveMySQLEndpoint = monitorMetaInfo.getSlaveMySQLEndpoint();
        Assert.assertEquals(6, slaveMySQLEndpoint.size());
        MetaKey metaKey = new MetaKey.Builder()
                .dc("dc1")
                .clusterId("dbcluster1.mha1dc1")
                .clusterName("dbcluster1")
                .mhaName("mha1dc1")
                .build();
        Assert.assertFalse(slaveMySQLEndpoint.containsKey(metaKey));
        prettyPrintJSON("slaveMySQLEndpoint", new JSONObject(slaveMySQLEndpoint).toString());
    }

    @Test
    public void testGetMasterReplicatorEndpoint() throws Exception {

        Drc drc = DefaultSaxParser.parse(DRC_XML);
        freshDrc(drc);

        drc.getDcs().get("dc1").getDbClusters().get("dbcluster1.mha1dc1").getReplicators().clear();
        drc.getDcs().get("dc1").getDbClusters().get("dbcluster1.mha2dc1").getReplicators().forEach(r -> r.setMaster(false));
        Mockito.doReturn(drc).when(dbClusterSourceProvider).getDrc();
        MonitorMetaInfo monitorMetaInfo = metaInfoServiceTwo.getMonitorMetaInfo();
        Map<MetaKey, Endpoint> masterReplicatorEndpoint = monitorMetaInfo.getMasterReplicatorEndpoint();
        Assert.assertEquals(6, masterReplicatorEndpoint.size());
        MetaKey metaKey = new MetaKey.Builder()
                .dc("dc1")
                .clusterId("dbcluster1.mha1dc1")
                .clusterName("dbcluster1")
                .mhaName("mha1dc1")
                .build();
        Assert.assertFalse(masterReplicatorEndpoint.containsKey(metaKey));
        prettyPrintJSON("masterReplicatorEndpoint", new JSONObject(masterReplicatorEndpoint).toString());
    }

    @Test
    public void testGetAllDataConsistencyMonitorTbl() throws SQLException {
        Mockito.doReturn(1L).when(metaInfoService).getMhaGroupId("fat-fx-drc1");
        Long mhaId1 = dalUtils.getId(TableEnum.MHA_TABLE, "fat-fx-drc1");
        Long mhaId2 = dalUtils.getId(TableEnum.MHA_TABLE, "fat-fx-drc2");
        MhaTbl mhaTbl1 = new MhaTbl();
        mhaTbl1.setId(mhaId1);
        mhaTbl1.setMhaName("fat-fx-drc1");
        MhaTbl mhaTbl2 = new MhaTbl();
        mhaTbl2.setId(mhaId2);
        mhaTbl2.setMhaName("fat-fx-drc2");
        Mockito.doReturn(Arrays.asList(mhaTbl1, mhaTbl2)).when(metaInfoService).getMhaTbls(1L);
        dalUtils.insertDataConsistencyMonitor(mhaId1.intValue(), "db1", "tbl1", "id", "datachange_lasttime");
        dalUtils.insertDataConsistencyMonitor(mhaId2.intValue(), "db2", "tbl2", "id", "datachange_lasttime");
        List<DataConsistencyMonitorTbl> consistencyMonitorTbls = dalUtils.getDataConsistencyMonitorTblDao().queryAll();
        for (DataConsistencyMonitorTbl consistencyMonitorTbl : consistencyMonitorTbls) {
            consistencyMonitorTbl.setMonitorSwitch(BooleanEnum.TRUE.getCode());
            dalUtils.updateDataConsistencyMonitor(consistencyMonitorTbl);
        }

        List<DataConsistencyMonitorTbl> dataConsistencyMonitorTbls = metaInfoServiceTwo.getAllDataConsistencyMonitorTbl("fat-fx-drc1");
        Assert.assertEquals(2, dataConsistencyMonitorTbls.size());
    }
}
