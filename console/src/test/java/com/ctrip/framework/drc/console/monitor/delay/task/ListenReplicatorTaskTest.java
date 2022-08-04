package com.ctrip.framework.drc.console.monitor.delay.task;

import com.ctrip.framework.drc.console.AllTests;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.MockTest;
import com.ctrip.framework.drc.console.monitor.comparator.ListeningReplicatorComparator;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorSlaveConfig;
import com.ctrip.framework.drc.console.monitor.delay.server.StaticDelayMonitorServer;
import com.ctrip.framework.drc.console.pojo.ReplicatorWrapper;
import com.ctrip.framework.drc.console.service.impl.DrcMaintenanceServiceImpl;
import com.ctrip.framework.drc.console.service.monitor.MonitorService;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.server.utils.RouteUtils;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
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

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.ctrip.framework.drc.console.utils.UTConstants.*;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-07-03
 */
public class ListenReplicatorTaskTest extends MockTest {

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

    @Mock
    private StaticDelayMonitorServer delayMonitorServer;

    @Mock
    private DefaultConsoleConfig consoleConfig;

    private Map<String, ReplicatorWrapper> oldReplicatorWrappers;

    private Map<String, ReplicatorWrapper> newReplicatorWrappers;

    private ListenReplicatorTask listenReplicatorTask;

    @Before
    public void setUp() throws Exception {
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

        String oldFile = ClassUtils.getDefaultClassLoader().getResource(XML_DELAY_MONITOR_DRC_OLD).getPath();
        String oldRouteDrcXmlStr = AllTests.readFileContent(oldFile);
        oldDrc = DefaultSaxParser.parse(oldRouteDrcXmlStr);

        String newFile = ClassUtils.getDefaultClassLoader().getResource(XML_DELAY_MONITOR_DRC_NEW).getPath();
        String newRouteDrcXmlStr = AllTests.readFileContent(newFile);
        newDrc = DefaultSaxParser.parse(newRouteDrcXmlStr);


        DelayMonitorSlaveConfig config = new DelayMonitorSlaveConfig();
        config.setEndpoint(new DefaultEndPoint("10.1.3.4", 8383));
        Mockito.when(delayMonitorServer.getConfig()).thenReturn(config);
        listenReplicatorTask = Mockito.spy(task);
        oldReplicatorWrappers = initReplicatorWrappers(oldDrc);
        newReplicatorWrappers = initReplicatorWrappers(newDrc);
        Mockito.doReturn(delayMonitorServer).when(listenReplicatorTask).createDelayMonitorServer(Mockito.any(DelayMonitorSlaveConfig.class));
        Mockito.when(consoleConfig.getDelayExceptionTime()).thenReturn(1000L);
        Mockito.when(monitorService.getMhaNamesToBeMonitored()).thenReturn(Lists.newArrayList("mhaToBeMonitored"));
        Mockito.when(dbClusterSourceProvider.getReplicatorsNeeded(Mockito.anyList())).thenReturn(newReplicatorWrappers);
        listenReplicatorTask.setReplicatorWrappers(oldReplicatorWrappers);
        Map<String, StaticDelayMonitorServer> delayMonitorServerMap = Maps.newConcurrentMap();
        delayMonitorServerMap.put("dbcluster3.mha3dc2", delayMonitorServer);
        delayMonitorServerMap.put("dbcluster2.mha3dc2", delayMonitorServer);
        delayMonitorServerMap.put("dbcluster1.mha1dc2", delayMonitorServer);
        delayMonitorServerMap.put("dbcluster1.mha2dc2", delayMonitorServer);
        listenReplicatorTask.setDelayMonitorServerMap(delayMonitorServerMap);
    }

    @Test
    public void testListeningReplicatorComparator() {
        ListeningReplicatorComparator comparator = new ListeningReplicatorComparator(oldReplicatorWrappers, newReplicatorWrappers);
        comparator.compare();
        Assert.assertEquals(1, comparator.getAdded().size());
        Assert.assertEquals(1, comparator.getRemoved().size());
        Assert.assertEquals(2, comparator.getMofified().size());
    }

    @Test
    public void testUpdateListenReplicator() throws Exception {
        listenReplicatorTask.updateListenReplicators();
        Thread.sleep(100);
        listenReplicatorTask.switchListenReplicator("dbcluster1.mha2dc2", "10.1.3.9", 8383);

        Thread.sleep(200);
        verify(delayMonitorServer, times(4)).initialize();
        verify(delayMonitorServer, times(4)).start();
        verify(delayMonitorServer, times(4)).stop();
        verify(delayMonitorServer, times(4)).dispose();
    }

    @Test
    public void testScheduleAndSwitchSimultaneously() throws Exception {
        ExecutorService executor = ThreadUtils.newFixedThreadPool(2, "executor");
        executor.submit(() -> {
            try {
                listenReplicatorTask.updateListenReplicators();
            } catch (SQLException e) {
            }
        });
        executor.submit(() -> listenReplicatorTask.switchListenReplicator("dbcluster1.mha2dc2", "10.1.3.2", 8383));

        Thread.sleep(200);
        verify(delayMonitorServer, atMost(4)).initialize();
        verify(delayMonitorServer, atMost(4)).start();
        verify(delayMonitorServer, atMost(4)).stop();
        verify(delayMonitorServer, atMost(4)).dispose();
    }

    private Map<String, ReplicatorWrapper> initReplicatorWrappers(Drc drc) {
        Map<String, ReplicatorWrapper> replicatorWrappers = Maps.newHashMap();
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
        return replicatorWrappers;
    }
}
