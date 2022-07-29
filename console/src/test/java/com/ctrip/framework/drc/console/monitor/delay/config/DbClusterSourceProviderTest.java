package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.console.AbstractTest;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.pojo.ReplicatorMonitorWrapper;
import com.ctrip.framework.drc.console.pojo.ReplicatorWrapper;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.*;

import static com.ctrip.framework.drc.console.AllTests.DRC_XML;
import static com.ctrip.framework.drc.console.monitor.MockTest.times;
import static com.ctrip.framework.drc.console.utils.UTConstants.DRC_XML_STR;
import static com.ctrip.framework.drc.console.utils.UTConstants.DRC_XML_STR2;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-22
 */
public class DbClusterSourceProviderTest extends AbstractTest {

    @InjectMocks
    private DbClusterSourceProvider dbClusterSourceProvider;

    @Mock
    private DataCenterService dataCenterService;

    @Mock
    private DaoConfig daoConfig;

    @Mock
    private RemoteConfig remoteConfig;

    @Mock
    private FileConfig fileConfig;

    @Mock
    private CompositeConfig compositeConfig;

    private static final String DC1= "dc1";

    private String expectedDrcString;

    private Drc expectedDrc;

    // deprecated
    private Drc tempDrc;
    // deprecated
    private Drc tempDrc2;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        expectedDrc = DefaultSaxParser.parse(DRC_XML);
        expectedDrcString = expectedDrc.toString();


        tempDrc = DefaultSaxParser.parse(DRC_XML_STR);
        tempDrc2 = DefaultSaxParser.parse(DRC_XML_STR2);

        dbClusterSourceProvider.drcString = null;
        dbClusterSourceProvider.drc = null;

        Mockito.doNothing().when(compositeConfig).updateConfig();
        Mockito.doReturn(expectedDrcString).when(compositeConfig).getConfig();
        Mockito.doReturn(DC1).when(dataCenterService).getDc();
    }

    @Test
    public void testInitializeAndScheduleTask() {
        dbClusterSourceProvider.initialize();
        dbClusterSourceProvider.scheduledTask();
        Assert.assertNotNull(dbClusterSourceProvider.drcString);
        Assert.assertNotNull(dbClusterSourceProvider.drc);
        Assert.assertEquals(expectedDrcString, dbClusterSourceProvider.drcString);
    }

    @Test
    public void testGetDrc() {
        Assert.assertNull(dbClusterSourceProvider.drcString);
        Assert.assertNull(dbClusterSourceProvider.drc);

        Drc drc = dbClusterSourceProvider.getDrc();
        Assert.assertNotNull(drc);
        Mockito.verify(compositeConfig, times(1)).updateConfig();

        drc = dbClusterSourceProvider.getDrc();
        Assert.assertNotNull(drc);
        Mockito.verify(compositeConfig, times(1)).updateConfig();

    }

    @Test
    public void testGetDrc2() {
        Drc actual = dbClusterSourceProvider.getDrc(DC1);
        Drc expect = new Drc().addDc(expectedDrc.findDc(DC1));
        System.out.println(actual.toString());
        Assert.assertTrue(StringUtils.isNotBlank(actual.toString()));
        Assert.assertEquals(expect.toString(), actual.toString());
    }

    @Test
    public void testGetLocalDrc() {
        Drc expect = new Drc().addDc(expectedDrc.findDc(DC1));
        Drc actual = dbClusterSourceProvider.getLocalDrc();
        System.out.println(actual.toString());
        Assert.assertTrue(StringUtils.isNotBlank(actual.toString()));
        Assert.assertEquals(expect.toString(), actual.toString());
    }

    @Test
    public void testGetDrcString() {
        Assert.assertNull(dbClusterSourceProvider.drcString);
        Assert.assertNull(dbClusterSourceProvider.drc);

        String drcString = dbClusterSourceProvider.getDrcString();
        Assert.assertNotNull(drcString);
        Mockito.verify(compositeConfig, times(1)).updateConfig();

        drcString = dbClusterSourceProvider.getDrcString();
        Assert.assertNotNull(drcString);
        Mockito.verify(compositeConfig, times(1)).updateConfig();

    }

    @Test
    public void testGetDc() {
        Dc actual = dbClusterSourceProvider.getDc(DC1);
        Dc expect = expectedDrc.findDc(DC1);
        System.out.println(actual.toString());
        Assert.assertTrue(StringUtils.isNotBlank(actual.toString()));
        Assert.assertEquals(expect.toString(), actual.toString());
    }

    @Test
    public void testGetLocalDc() {
        Dc actual = dbClusterSourceProvider.getLocalDc();
        Dc expect = expectedDrc.findDc(DC1);
        System.out.println(actual.toString());
        Assert.assertTrue(StringUtils.isNotBlank(actual.toString()));
        Assert.assertEquals(expect.toString(), actual.toString());
    }

    @Test
    public void testGetDrcFromQConfig() throws IOException, SAXException {
        Drc actual = dbClusterSourceProvider.getDrcFromQConfig();
        String s = "<?xml version=\"1.0\" encoding=\"utf-8\"?><drc><dc id=\"dc1\"></dc></drc>";
        Drc expect = DefaultSaxParser.parse(s);
        System.out.println(actual.toString());
        Assert.assertTrue(StringUtils.isNotBlank(actual.toString()));
        Assert.assertEquals(expect.toString(), actual.toString());
    }

    @Test
    public void testGetDcs() {
        Map<String, Dc> dcs = dbClusterSourceProvider.getDcs();
        Assert.assertEquals(3, dcs.size());
    }

    @Test
    public void testGetMhaGroups() throws Exception {
        List<Set<DbClusterSourceProvider.Mha>> actual = dbClusterSourceProvider.getMhaGroups();
        Map<String, Dc> dcs = expectedDrc.getDcs();
        Dc dc1 = dcs.get("dc1");
        Dc dc2 = dcs.get("dc2");
        Dc dc3 = dcs.get("dc3");
        List<Set<DbClusterSourceProvider.Mha>> expected = new ArrayList<>() {{
            add(new HashSet<>() {{
                add(new DbClusterSourceProvider.Mha("dc1", dc1.getDbClusters().get("dbcluster1.mha1dc1")));
                add(new DbClusterSourceProvider.Mha("dc2", dc2.getDbClusters().get("dbcluster1.mha1dc2")));
            }});
            add(new HashSet<>() {{
                add(new DbClusterSourceProvider.Mha("dc1", dc1.getDbClusters().get("dbcluster1.mha2dc1")));
                add(new DbClusterSourceProvider.Mha("dc2", dc2.getDbClusters().get("dbcluster1.mha2dc2")));
            }});
            add(new HashSet<>() {{
                add(new DbClusterSourceProvider.Mha("dc1", dc1.getDbClusters().get("dbcluster2.mha3dc1")));
                add(new DbClusterSourceProvider.Mha("dc2", dc2.getDbClusters().get("dbcluster2.mha3dc2")));
                add(new DbClusterSourceProvider.Mha("dc3", dc3.getDbClusters().get("dbcluster2.mha3dc3")));
            }});
        }};
        Assert.assertEquals(3, actual.size());
        Assert.assertEquals(expected, actual);
    }
    @Test
    public void testGetMhaGroupPairs() throws Exception {
        Map<String,List<DbClusterSourceProvider.Mha>> actual = dbClusterSourceProvider.getMhaGroupPairs();
        Assert.assertEquals(4, actual.size());
    }

    @Test
    public void testGetAllMhas() {
        Set<DbClusterSourceProvider.Mha> actual = dbClusterSourceProvider.getAllMhas();
        Assert.assertEquals(7, actual.size());
        Set<DbClusterSourceProvider.Mha> expect = new HashSet<>();
        for(Map.Entry<String, Dc> entry : expectedDrc.getDcs().entrySet()) {
            String dcId = entry.getKey();
            Dc dc = entry.getValue();
            dc.getDbClusters().values().forEach(dbCluster -> { expect.add(new DbClusterSourceProvider.Mha(dcId, dbCluster)); });
        }
        Assert.assertEquals(expect, actual);
    }

    @Test
    public void testGetReplicatorsNotInLocalDc() {
        dbClusterSourceProvider.drc = tempDrc;
        dbClusterSourceProvider.localDc = "ntgxh";
        List<String> mhaNamesToBeMonitored = Lists.newArrayList("drcNt", "drcNt2", "drcOy", "drcOy2", "drcRb", "drcRb2");
        Map<String, ReplicatorWrapper> replicators = dbClusterSourceProvider.getReplicatorsNotInSrcDc(mhaNamesToBeMonitored,"ntgxh");
        Assert.assertEquals(4, replicators.keySet().size());
        ReplicatorWrapper replicator = replicators.get("drc-Test01.drcOy");
        Assert.assertEquals("127.0.0.2", replicator.getIp());
        Assert.assertEquals(8383, replicator.getPort());
        Assert.assertEquals("drc-Test01", replicator.getClusterName());
        Assert.assertEquals("ntgxh", replicator.getDcName());
        Assert.assertEquals("shaoy", replicator.getDestDcName());
        Assert.assertEquals("drcNt", replicator.getMhaName());
        Assert.assertEquals("drcOy", replicator.getDestMhaName());
        Assert.assertEquals("PROXYTCP://127.0.0.28:80,PROXYTCP://127.0.0.82:80,PROXYTCP://127.0.0.135:80,PROXYTCP://127.0.0.188:80 PROXYTLS://127.0.0.8:443,PROXYTLS://127.0.0.11:443", replicator.getRoutes().get(0).getRouteInfo());
        replicator = replicators.get("drc-Test02.drcOy2");
        Assert.assertEquals("127.0.0.2", replicator.getIp());
        Assert.assertEquals(8383, replicator.getPort());
        Assert.assertEquals("drc-Test02", replicator.getClusterName());
        Assert.assertEquals("ntgxh", replicator.getDcName());
        Assert.assertEquals("shaoy", replicator.getDestDcName());
        Assert.assertEquals("drcNt2", replicator.getMhaName());
        Assert.assertEquals("drcOy2", replicator.getDestMhaName());
        Assert.assertEquals("PROXYTCP://127.0.0.28:80,PROXYTCP://127.0.0.82:80,PROXYTCP://127.0.0.135:80,PROXYTCP://127.0.0.188:80 PROXYTLS://127.0.0.8:443,PROXYTLS://127.0.0.11:443", replicator.getRoutes().get(0).getRouteInfo());
        replicator = replicators.get("drc-Test01.drcRb");
        Assert.assertEquals("127.0.0.3", replicator.getIp());
        Assert.assertEquals(8383, replicator.getPort());
        Assert.assertEquals("drc-Test01", replicator.getClusterName());
        Assert.assertEquals("ntgxh", replicator.getDcName());
        Assert.assertEquals("sharb", replicator.getDestDcName());
        Assert.assertEquals("drcNt", replicator.getMhaName());
        Assert.assertEquals("drcRb", replicator.getDestMhaName());
        Assert.assertEquals(0, replicator.getRoutes().size());
        replicator = replicators.get("drc-Test02.drcRb2");
        Assert.assertEquals("127.0.0.3", replicator.getIp());
        Assert.assertEquals(8383, replicator.getPort());
        Assert.assertEquals("drc-Test02", replicator.getClusterName());
        Assert.assertEquals("ntgxh", replicator.getDcName());
        Assert.assertEquals("sharb", replicator.getDestDcName());
        Assert.assertEquals("drcNt2", replicator.getMhaName());
        Assert.assertEquals("drcRb2", replicator.getDestMhaName());
        Assert.assertEquals(0, replicator.getRoutes().size());
    }

    @Test
    public void testGetReplicatorMonitorsInLocalDc() {
        dbClusterSourceProvider.drcFromQConfig = tempDrc;
        dbClusterSourceProvider.localDc = "ntgxh";
        List<ReplicatorMonitorWrapper> replicatorMonitors = dbClusterSourceProvider.getReplicatorMonitorsInLocalDc();
        ReplicatorMonitorWrapper replicatorMonitor = replicatorMonitors.get(0);
        Assert.assertEquals("127.0.0.1", replicatorMonitor.getIp());
        Assert.assertEquals(18383, replicatorMonitor.getPort());
        Assert.assertEquals("drc-Test01", replicatorMonitor.getClusterName());
        Assert.assertEquals("ntgxh", replicatorMonitor.getDcName());
        Assert.assertEquals("ntgxh", replicatorMonitor.getDestDcName());
        Assert.assertEquals("drcNt", replicatorMonitor.getMhaName());
        Assert.assertEquals("drcNt", replicatorMonitor.getDestMhaName());
        replicatorMonitor = replicatorMonitors.get(1);
        Assert.assertEquals("127.0.0.1", replicatorMonitor.getIp());
        Assert.assertEquals(18383, replicatorMonitor.getPort());
        Assert.assertEquals("drc-Test02", replicatorMonitor.getClusterName());
        Assert.assertEquals("ntgxh", replicatorMonitor.getDcName());
        Assert.assertEquals("ntgxh", replicatorMonitor.getDestDcName());
        Assert.assertEquals("drcNt2", replicatorMonitor.getMhaName());
        Assert.assertEquals("drcNt2", replicatorMonitor.getDestMhaName());
    }

    @Test
    public void testGetMasterDbEndpointInLocalDc() throws Exception {
        dbClusterSourceProvider.drc = tempDrc;
        dbClusterSourceProvider.localDc = "ntgxh";
        Map<String, Endpoint> expected = Maps.newHashMap();
        expected.put("drcNt", new MySqlEndpoint("127.0.0.1", 3306, "mroot", "mpassword", BooleanEnum.TRUE.isValue()));
        expected.put("drcNt2", new MySqlEndpoint("127.0.0.1", 4406, "mroot", "mpassword", BooleanEnum.TRUE.isValue()));
        Map<String, Endpoint> actual = dbClusterSourceProvider.getMasterDbEndpointInLocalDc();
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testGetTargetDcMha() {
        dbClusterSourceProvider.drc = tempDrc2;
        List<String> targetDcMha = dbClusterSourceProvider.getTargetDcMha("drcOy");
        Assert.assertNotNull(targetDcMha);
        Assert.assertEquals(2, targetDcMha.size());
        Assert.assertEquals("sharb", targetDcMha.get(0));
        Assert.assertEquals("drcRb", targetDcMha.get(1));

        targetDcMha = dbClusterSourceProvider.getTargetDcMha("drcRb");
        Assert.assertNotNull(targetDcMha);
        Assert.assertEquals(2, targetDcMha.size());
        Assert.assertEquals("shaoy", targetDcMha.get(0));
        Assert.assertEquals("drcOy", targetDcMha.get(1));
    }

    @Test
    public void testGetMaster() {
        dbClusterSourceProvider.drc = tempDrc;
        Map<String, Dc> dcs = tempDrc.getDcs();
        Dc ntgxh = dcs.get("ntgxh");
        DbCluster dbCluster = ntgxh.getDbClusters().get("drc-Test01.drcNt");
        Endpoint actual = dbClusterSourceProvider.getMaster(dbCluster);
        Assert.assertEquals("127.0.0.1", actual.getHost());
        Assert.assertEquals(3306, actual.getPort());
        Assert.assertEquals("mroot", actual.getUser());
        Assert.assertEquals("mpassword", actual.getPassword());
    }

    @Test
    public void testGetMasterEndpoint() throws Exception {
        dbClusterSourceProvider.drc = tempDrc;
        Endpoint actual = dbClusterSourceProvider.getMasterEndpoint("drcNt");
        Assert.assertEquals("127.0.0.1", actual.getHost());
        Assert.assertEquals(3306, actual.getPort());
        Assert.assertEquals("mroot", actual.getUser());
        Assert.assertEquals("mpassword", actual.getPassword());

        actual = dbClusterSourceProvider.getMasterEndpoint("noSuchMha");
        Assert.assertNull(actual);
    }

    @Test
    public void testGetCombinationListFromSet() {
        dbClusterSourceProvider.drc = tempDrc;
        Map<String, Dc> dcs = tempDrc.getDcs();
        Dc ntgxh = dcs.get("ntgxh");
        Dc shaoy = dcs.get("shaoy");
        Dc sharb = dcs.get("sharb");
        Set<DbClusterSourceProvider.Mha> mhaGroup = new HashSet<>() {{
            add(new DbClusterSourceProvider.Mha("ntgxh", ntgxh.getDbClusters().get("drc-Test01.drcNt")));
            add(new DbClusterSourceProvider.Mha("shaoy", shaoy.getDbClusters().get("drc-Test01.drcOy")));
            add(new DbClusterSourceProvider.Mha("sharb", sharb.getDbClusters().get("drc-Test01.drcRb")));
        }};
        List<List<DbClusterSourceProvider.Mha>> actual = dbClusterSourceProvider.getCombinationListFromSet(mhaGroup);

        Assert.assertEquals(3, actual.size());
        Map<DbClusterSourceProvider.Mha, Integer> mhaCounter = new HashMap<>();
        for(List<DbClusterSourceProvider.Mha> mhaCombination : actual) {
            Assert.assertEquals(2, mhaCombination.size());
            DbClusterSourceProvider.Mha mha1 = mhaCombination.get(0);
            DbClusterSourceProvider.Mha mha2 = mhaCombination.get(1);
            Assert.assertNotEquals(mha1, mha2);
            int newCount = mhaCounter.containsKey(mha1) ? mhaCounter.get(mha1) + 1 : 1;
            mhaCounter.put(mha1, newCount);
            newCount = mhaCounter.containsKey(mha2) ? mhaCounter.get(mha2) + 1 : 1;
            mhaCounter.put(mha2, newCount);
        }
        for(int count : mhaCounter.values()) {
            Assert.assertEquals(2, count);
        }
    }

    @Test
    public void testGetAllMhaCombinationList() throws Exception {
        dbClusterSourceProvider.drc = tempDrc;
        List<List<DbClusterSourceProvider.Mha>> actual = dbClusterSourceProvider.getAllMhaCombinationList();

        Assert.assertEquals(6, actual.size());
        Map<DbClusterSourceProvider.Mha, Integer> mhaCounter = new HashMap<>();
        for(List<DbClusterSourceProvider.Mha> mhaCombination : actual) {
            Assert.assertEquals(2, mhaCombination.size());
            DbClusterSourceProvider.Mha mha1 = mhaCombination.get(0);
            DbClusterSourceProvider.Mha mha2 = mhaCombination.get(1);
            Assert.assertNotEquals(mha1, mha2);
            int newCount = mhaCounter.containsKey(mha1) ? mhaCounter.get(mha1) + 1 : 1;
            mhaCounter.put(mha1, newCount);
            newCount = mhaCounter.containsKey(mha2) ? mhaCounter.get(mha2) + 1 : 1;
            mhaCounter.put(mha2, newCount);
        }
        for(int count : mhaCounter.values()) {
            Assert.assertEquals(2, count);
        }
    }

    @Test
    public void testMhaGetMasterDb() {
        DbCluster dbCluster = tempDrc.getDcs().values().stream().findFirst().get().getDbClusters().values().stream().findFirst().get();
        DbClusterSourceProvider.Mha drcNt = new DbClusterSourceProvider.Mha("ntgxh", dbCluster);
        Endpoint actual = drcNt.getMasterDb();
        Endpoint expected = new MySqlEndpoint("127.0.0.1", 3306, "mroot", "mpassword", BooleanEnum.TRUE.isValue());
        Assert.assertEquals(expected, actual);
    }
}
