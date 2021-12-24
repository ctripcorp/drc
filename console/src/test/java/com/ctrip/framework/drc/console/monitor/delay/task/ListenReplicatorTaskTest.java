package com.ctrip.framework.drc.console.monitor.delay.task;

import com.ctrip.framework.drc.console.AllTests;
import com.ctrip.framework.drc.console.monitor.comparator.ListeningReplicatorComparator;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorSlaveConfig;
import com.ctrip.framework.drc.console.pojo.ReplicatorWrapper;
import com.ctrip.framework.drc.console.service.impl.DrcMaintenanceServiceImpl;
import com.ctrip.framework.drc.console.service.monitor.MonitorService;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.server.utils.RouteUtils;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.google.common.collect.Maps;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.util.ClassUtils;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.console.utils.UTConstants.XML_OLD_ROUTE_DRC;
import static com.ctrip.framework.drc.console.utils.UTConstants.XML_NEW_ROUTE_DRC;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-07-03
 */
public class ListenReplicatorTaskTest {

    @InjectMocks
    private ListenReplicatorTask task = new ListenReplicatorTask();

    private Drc drc;

    private Drc oldDrc;

    private Drc newDrc;

    @Mock
    private DrcMaintenanceServiceImpl drcMaintenanceService;

    @Mock
    private DbClusterSourceProvider dbClusterSourceProvider;

    @Mock
    private MonitorService monitorService;

    @Before
    public void setUp() throws IOException, SAXException {
        MockitoAnnotations.openMocks(this);

        String drcXmlStr = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
                "<drc>\n" +
                "    <dc id=\"shaoy\">\n" +
                "        <clusterManager ip=\"10.2.84.122\" port=\"8080\" master=\"true\"/>\n" +
                "        <zkServer address=\"10.2.84.112:2181\"/>\n" +
                "        <dbClusters>\n" +
                "            <dbCluster id=\"integration-test.drcOy\" name=\"integration-test\" mhaName=\"drcOy\" buName=\"BBZ\" appId=\"100024819\">\n" +
                "                <dbs readUser=\"root\" readPassword=\"root\" writeUser=\"root\" writePassword=\"root\" monitorUser=\"root\" monitorPassword=\"root\">\n" +
                "                    <db ip=\"10.2.83.109\" port=\"3306\" master=\"true\" uuid=\"bd9b313c-b56d-11ea-825d-fa163e02998c\"/>\n" +
                "                    <db ip=\"10.2.83.110\" port=\"3306\" master=\"false\" uuid=\"b73fd53a-b56d-11ea-ac10-fa163ec90ff6\"/>\n" +
                "                </dbs>\n" +
                "                <replicator ip=\"10.2.83.105\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"9534ea47-b56d-11ea-b18f-fa163eaa9d69:1-26\"/>\n" +
                "                <replicator ip=\"10.2.63.130\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"9534ea47-b56d-11ea-b18f-fa163eaa9d69:1-26\"/>\n" +
                "                <applier ip=\"10.2.83.100\" port=\"8080\" targetIdc=\"sharb\" targetMhaName=\"drcRb\" gtidExecuted=\"bd9b313c-b56d-11ea-825d-fa163e02998c:1-26\"/>\n" +
                "                <applier ip=\"10.2.86.137\" port=\"8080\" targetIdc=\"sharb\" targetMhaName=\"drcRb\" gtidExecuted=\"bd9b313c-b56d-11ea-825d-fa163e02998c:1-26\"/>\n" +
                "            </dbCluster>\n" +
                "        </dbClusters>\n" +
                "    </dc>\n" +
                "    <dc id=\"sharb\">\n" +
                "        <clusterManager ip=\"10.2.84.109\" port=\"8080\" master=\"true\"/>\n" +
                "        <zkServer address=\"10.2.83.114:2181\"/>\n" +
                "        <dbClusters>\n" +
                "            <dbCluster id=\"integration-test.drcRb\" name=\"integration-test\" mhaName=\"drcRb\" buName=\"BBZ\" appId=\"100024819\">\n" +
                "                <dbs readUser=\"root\" readPassword=\"root\" writeUser=\"root\" writePassword=\"root\" monitorUser=\"root\" monitorPassword=\"root\">\n" +
                "                    <db ip=\"10.2.83.107\" port=\"3306\" master=\"true\" uuid=\"9534ea47-b56d-11ea-b18f-fa163eaa9d69\"/>\n" +
                "                    <db ip=\"10.2.83.108\" port=\"3306\" master=\"false\" uuid=\"a4f134a7-b56d-11ea-a7ab-fa163e2d32b8\"/>\n" +
                "                </dbs>\n" +
                "                <replicator ip=\"10.2.83.106\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"bd9b313c-b56d-11ea-825d-fa163e02998c:1-26\"/>\n" +
                "                <replicator ip=\"10.2.86.199\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"bd9b313c-b56d-11ea-825d-fa163e02998c:1-26\"/>\n" +
                "                <applier ip=\"10.2.83.111\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"drcOy\" gtidExecuted=\"9534ea47-b56d-11ea-b18f-fa163eaa9d69:1-26\"/>\n" +
                "                <applier ip=\"10.2.86.136\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"drcOy\" gtidExecuted=\"9534ea47-b56d-11ea-b18f-fa163eaa9d69:1-26\"/>\n" +
                "            </dbCluster>\n" +
                "        </dbClusters>\n" +
                "    </dc>\n" +
                "</drc>";
        drc = DefaultSaxParser.parse(drcXmlStr);

        String oldFile = ClassUtils.getDefaultClassLoader().getResource(XML_OLD_ROUTE_DRC).getPath();
        String oldRouteDrcXmlStr = AllTests.readFileContent(oldFile);
        oldDrc = DefaultSaxParser.parse(oldRouteDrcXmlStr);

        String newFile = ClassUtils.getDefaultClassLoader().getResource(XML_NEW_ROUTE_DRC).getPath();
        String newRouteDrcXmlStr = AllTests.readFileContent(newFile);
        newDrc = DefaultSaxParser.parse(newRouteDrcXmlStr);
    }

    @Test
    public void testUpdateMasterReplicator() {
        DelayMonitorSlaveConfig config = new DelayMonitorSlaveConfig();
        config.setDestMha("");
        config.setEndpoint(new DefaultEndPoint("", 3306));
        task.updateMasterReplicator(config, "ip:3336");
        Assert.assertTrue(true);
    }

//    @Test
//    public void testCheckMaster() {
//        Replicator replicator105 = drc.getDcs().get("shaoy").getDbClusters().get("integration-test.drcOy").getReplicators().get(0);
//        Replicator replicator130 = drc.getDcs().get("shaoy").getDbClusters().get("integration-test.drcOy").getReplicators().get(1);
//        DbClusterSourceProvider.ReplicatorWrapper replicatorWrapper105 = new DbClusterSourceProvider.ReplicatorWrapper(replicator105, "sharb", "shaoy", "integration-test", "drcRb", "drcOy");
//        DbClusterSourceProvider.ReplicatorWrapper replicatorWrapper130 = new DbClusterSourceProvider.ReplicatorWrapper(replicator130, "sharb", "shaoy", "integration-test", "drcRb", "drcOy");
//
//        task.setReplicatorWrappers(new HashMap<>() {{
//            put("integration-test.drcOy", replicatorWrapper105);
//        }});
//        boolean b = task.checkMaster("integration-test.drcOy", replicatorWrapper105);
//        logger.info("{}:{} master:{}", replicatorWrapper105.getIp(), replicatorWrapper105.getPort(), b);
////        // manual test
////        Assert.assertEquals(replicatorWrapper105.getIp(), task.getReplicatorWrappers().get("integration-test.drcOy").getIp());
////        Assert.assertEquals(replicatorWrapper105.getPort(), task.getReplicatorWrappers().get("integration-test.drcOy").getPort());
////        Assert.assertTrue(task.checkMaster("integration-test.drcOy", replicatorWrapper105));
////        Assert.assertEquals(replicatorWrapper105.getIp(), task.getReplicatorWrappers().get("integration-test.drcOy").getIp());
////        Assert.assertEquals(replicatorWrapper105.getPort(), task.getReplicatorWrappers().get("integration-test.drcOy").getPort());
//
//        task.setReplicatorWrappers(new HashMap<>() {{
//            put("integration-test.drcOy", replicatorWrapper130);
//        }});
//        boolean b1 = task.checkMaster("integration-test.drcOy", replicatorWrapper130);
//        logger.info("{}:{} master:{}", replicatorWrapper130.getIp(), replicatorWrapper130.getPort(), b);
////        // manual test
////        Assert.assertEquals(replicatorWrapper130.getIp(), task.getReplicatorWrappers().get("integration-test.drcOy").getIp());
////        Assert.assertEquals(replicatorWrapper130.getPort(), task.getReplicatorWrappers().get("integration-test.drcOy").getPort());
////        Assert.assertFalse(task.checkMaster("integration-test.drcOy", replicatorWrapper130));
////        Assert.assertEquals(replicatorWrapper105.getIp(), task.getReplicatorWrappers().get("integration-test.drcOy").getIp());
////        Assert.assertEquals(replicatorWrapper105.getPort(), task.getReplicatorWrappers().get("integration-test.drcOy").getPort());
//    }

    @Test
    public void testUpdateListenReplicator() throws SQLException {
        Map<String, ReplicatorWrapper> oldReplicatorWrappers = Maps.newHashMap();
        Map<String, ReplicatorWrapper> newReplicatorWrappers = Maps.newHashMap();

        initReplicatorWrappers(oldDrc, oldReplicatorWrappers);
        initReplicatorWrappers(newDrc, newReplicatorWrappers);

        ListenReplicatorTask listenReplicatorTask = Mockito.spy(task);
        Mockito.doNothing().when(listenReplicatorTask).startListenServer(Mockito.anyString(), Mockito.any(ReplicatorWrapper.class));
        Mockito.doNothing().when(listenReplicatorTask).stopListenServer(Mockito.anyString());

        Mockito.when(monitorService.getMhaNamesToBeMonitored()).thenReturn(Lists.newArrayList("mhaToBeMonitored"));
        Mockito.when(dbClusterSourceProvider.getReplicatorsNotInLocalDc(Mockito.anyList())).thenReturn(newReplicatorWrappers);
        listenReplicatorTask.setReplicatorWrappers(oldReplicatorWrappers);

        listenReplicatorTask.updateListenReplicators();

        ListeningReplicatorComparator comparator = new ListeningReplicatorComparator(oldReplicatorWrappers, newReplicatorWrappers);
        comparator.compare();
        Assert.assertEquals(1, comparator.getAdded().size());
        Assert.assertEquals(1, comparator.getRemoved().size());
        Assert.assertEquals(3, comparator.getMofified().size());
    }

    private void initReplicatorWrappers(Drc drc, Map<String, ReplicatorWrapper> replicatorWrappers) {
        Dc dc1 = drc.getDcs().get("dc1");
        Dc dc2 = drc.getDcs().get("dc2");
        Map<String, DbCluster> dbClusters = dc2.getDbClusters();
        for(Map.Entry<String, DbCluster> entry : dbClusters.entrySet()) {
            String dbClusterId = entry.getKey();
            DbCluster dbCluster = entry.getValue();
            List<Applier> appliers = dbCluster.getAppliers();
            Applier applierChoose = null;
            for(Applier applier : appliers) {
                if(applier.getTargetIdc().equalsIgnoreCase("dc1")) {
                    applierChoose = applier;
                    break;
                }
            }

            List<Route> routes = RouteUtils.filterRoutes("dc1", Route.TAG_CONSOLE, dbCluster.getOrgId(), "dc2", dc1);
            replicatorWrappers.put(dbClusterId,
                    new ReplicatorWrapper(dbCluster.getReplicators().stream().filter(Replicator::isMaster).findFirst().orElse(dbCluster.getReplicators().get(0)),
                            "dc1",
                            "dc2",
                            dbCluster.getName(),
                            applierChoose.getTargetMhaName(),
                            dbCluster.getMhaName(),
                            routes
                    )
            );
        }
    }
}
