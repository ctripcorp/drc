package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierInfoDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.core.utils.NameUtils;
import com.ctrip.framework.drc.manager.enums.ServerStateEnum;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.ha.meta.RegionCache;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.zk.ZkClient;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_MQ;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;


/**
 * Created by jixinwang on 2022/11/16
 */
public class MessengerInstanceManagerTest extends AbstractDbClusterTest {

    @InjectMocks
    private MessengerInstanceManager messengerInstanceManager = new MessengerInstanceManager();

    @Mock
    private ZkClient zkClient;

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Mock
    private RegionCache regionCache;

    @Mock
    private InstanceStateController instanceStateController;

    @Mock
    private InstanceActiveElectAlgorithmManager instanceActiveElectAlgorithmManager;

    @Mock
    protected ClusterManagerConfig clusterManagerConfig;

    @Mock
    private DefaultClusterManagers clusterServers;
    @Mock
    private ClusterServerStateManager clusterServerStateManager;


    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        super.setUp();
        when(zkClient.get()).thenReturn(curatorFramework);
        when(currentMetaManager.watchMessengerIfNotWatched(anyString())).thenReturn(true);
        when(instanceActiveElectAlgorithmManager.get(anyString())).thenReturn(new DefaultInstanceActiveElectAlgorithm());
        when(regionCache.getCluster(anyString())).thenReturn(dbCluster);
        when(clusterServerStateManager.getServerState()).thenReturn(ServerStateEnum.NORMAL);

        Messenger messenger = dbCluster.getMessengers().get(0);
        zookeeperValue.setPort(messenger.getPort());
        zookeeperValue.setIp(messenger.getIp());
        messengerInstanceManager.initialize();
        messengerInstanceManager.start();
    }

    @After
    public void tearDown() {
        try {
            if (persistentNode != null) {
                persistentNode.close();
            }
            messengerInstanceManager.stop();
            messengerInstanceManager.dispose();
        } catch (Exception e) {

        }
    }


    @Test
    public void handleClusterAdded() throws InterruptedException {
        messengerInstanceManager.update(new NodeAdded<>(dbCluster), null);
        verify(instanceStateController, times(1)).registerMessenger(anyString(), anyObject());
    }

    @Test
    public void handleClusterDelete() {
        messengerInstanceManager.update(new NodeDeleted<>(dbCluster), null);
        verify(instanceStateController, times(1)).removeMessenger(dbCluster.getId(), dbCluster.getMessengers().get(0), true);
    }

    @Test
    public void handleClusterModify() {
        DbCluster cloneDbCluster = MetaClone.clone(dbCluster);
        newMessenger.setIp("12.21.12.21");
        newMessenger.setPort(4321);
        newMessenger.setApplyMode(ApplyMode.mq.getType());
        cloneDbCluster.getMessengers().clear();
        cloneDbCluster.getMessengers().add(newMessenger);
        ClusterComparator clusterComparator = new ClusterComparator(dbCluster, cloneDbCluster);
        clusterComparator.compare();
        messengerInstanceManager.update(clusterComparator, null);
        verify(instanceStateController, times(1)).removeMessenger(dbCluster.getId(), dbCluster.getMessengers().get(0), true);
    }

    @Test
    public void handleClusterModified() {
        String clusterId = "test_id";

        Dbs dbs = new Dbs();
        Db db = new Db();
        dbs.addDb(db);

        DbCluster current = new DbCluster();
        current.setDbs(dbs);
        current.setId(clusterId);
        Messenger messenger = new Messenger();
        messenger.setIp("127.0.0.1");
        messenger.setPort(8080);
        messenger.setApplyMode(ApplyMode.mq.getType());
        current.addMessenger(messenger);

        DbCluster future = new DbCluster();
        future.setDbs(dbs);
        future.setId(clusterId);
        Messenger messenger2 = new Messenger();
        messenger2.setProperties("test_property");
        messenger2.setIp("127.0.0.1");
        messenger2.setPort(8080);
        messenger2.setApplyMode(ApplyMode.mq.getType());
        future.addMessenger(messenger2);

        ClusterComparator comparator = new ClusterComparator(current, future);
        comparator.compare();

        Mockito.when(currentMetaManager.getActiveMessenger(Mockito.anyString(), Mockito.anyString())).thenReturn(messenger);
        Mockito.when(clusterManagerConfig.checkApplierProperty()).thenReturn(true);

        messengerInstanceManager.handleClusterModified(comparator);

        Mockito.verify(instanceStateController, times(1)).messengerPropertyChange(clusterId, messenger2);
    }


    @Test
    public void testChecker() {
        MessengerInstanceManager.MessengerChecker checker = messengerInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto("mha1_dc2_dalcluster.mha1_dc2", "db1", "127.0.2.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto("mha1_dc2_dalcluster.mha1_dc2", "db1", "127.0.2.2", 8080, false, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(instanceStateController.getMessengerInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeMessenger(any(), any(), anyBoolean());
        verify(instanceStateController, never()).addMessenger(any(), any());
        verify(instanceStateController, never()).registerMessenger(any(), any());
    }

    @Test
    public void testCheckerRegister() {
        MessengerInstanceManager.MessengerChecker checker = messengerInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(instanceStateController.getMessengerInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeMessenger(any(), any(), anyBoolean());
        verify(instanceStateController, never()).addMessenger(any(), any());
        verify(instanceStateController, times(4)).registerMessenger(any(), any());
    }

    @Test
    public void testCheckerFailHttp() {
        MessengerInstanceManager.MessengerChecker checker = messengerInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto("mha1_dc2_dalcluster.mha1_dc2", "db1", "127.0.2.1", 8080, true, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1");
        when(instanceStateController.getMessengerInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeMessenger(any(), any(), anyBoolean());
        verify(instanceStateController, never()).addMessenger(any(), any());
        verify(instanceStateController, never()).registerMessenger(any(), any());
    }

    @Test
    public void testCheckerFailHttp2() {
        MessengerInstanceManager.MessengerChecker checker = messengerInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto("mha1_dc2_dalcluster.mha1_dc2", "db1", "127.0.2.1", 8080, true, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.3");
        when(instanceStateController.getMessengerInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeMessenger(any(), any(), anyBoolean());
        verify(instanceStateController, never()).addMessenger(any(), any());
        verify(instanceStateController, never()).registerMessenger(any(), any());
    }


    @Test
    public void testCheckerWrongMaster() {
        MessengerInstanceManager.MessengerChecker checker = messengerInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto("mha1_dc2_dalcluster.mha1_dc2", "db1", "127.0.2.1", 8080, false, "10.1.1.1"),
                getApplierInfoDto("mha1_dc2_dalcluster.mha1_dc2", "db1", "127.0.2.2", 8080, true, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(instanceStateController.getMessengerInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeMessenger(any(), any(), anyBoolean());
        verify(instanceStateController, times(1)).addMessenger(any(), any());
        verify(instanceStateController, never()).registerMessenger(any(), any());
    }

    @Test
    public void testCheckerTwoMaster() {
        init();
        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto("mha1_dc2_dalcluster.mha1_dc2", "db1", "127.0.2.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto("mha1_dc2_dalcluster.mha1_dc2", "db1", "127.0.2.2", 8080, true, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(instanceStateController.getMessengerInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));

        MessengerInstanceManager.MessengerChecker checker = messengerInstanceManager.getChecker();
        checker.run();

        verify(instanceStateController, never()).removeMessenger(any(), any(), anyBoolean());
        verify(instanceStateController, times(1)).addMessenger(any(), any());
        verify(instanceStateController, never()).registerMessenger(any(), any());
    }

    @Test
    public void testCheckerWrongReplicator() {
        MessengerInstanceManager.MessengerChecker checker = messengerInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto("mha1_dc2_dalcluster.mha1_dc2", "db1", "127.0.2.1", 8080, true, "10.1.1.2"),
                getApplierInfoDto("mha1_dc2_dalcluster.mha1_dc2", "db1", "127.0.2.2", 8080, false, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(instanceStateController.getMessengerInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeMessenger(any(), any(), anyBoolean());
        verify(instanceStateController, times(1)).addMessenger(any(), any());
        verify(instanceStateController, never()).registerMessenger(any(), any());
    }


    @Test
    public void testCheckerRedundantMessenger() {
        MessengerInstanceManager.MessengerChecker checker = messengerInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto("mha1_dc2_dalcluster.mha1_dc2", "db1", "127.0.2.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto("mha1_dc2_dalcluster.mha1_dc2", "db1", "127.0.2.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto("mha1_dc2_dalcluster.mha1_dc2", "db1", "127.0.2.3", 8080, false, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2", "127.0.2.3");
        when(instanceStateController.getMessengerInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, times(1)).removeMessenger(any(), any(), anyBoolean());
        verify(instanceStateController, never()).addMessenger(any(), any());
        verify(instanceStateController, never()).registerMessenger(any(), any());
    }

    @Test
    public void testCheckerLackMessenger() {
        MessengerInstanceManager.MessengerChecker checker = messengerInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto("mha1_dc2_dalcluster.mha1_dc2", "db1", "127.0.2.1", 8080, true, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(instanceStateController.getMessengerInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeMessenger(any(), any(), anyBoolean());
        verify(instanceStateController, never()).addMessenger(any(), any());
        verify(instanceStateController, times(1)).registerMessenger(any(), any());
    }


    @Test
    public void testCheckerLackMasterMessenger() {
        MessengerInstanceManager.MessengerChecker checker = messengerInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto("mha1_dc2_dalcluster.mha1_dc2", "db1", "127.0.2.1", 8080, false, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(instanceStateController.getMessengerInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeMessenger(any(), any(), anyBoolean());
        verify(instanceStateController, never()).addMessenger(any(), any());
        verify(instanceStateController, times(1)).registerMessenger(any(), any());
    }

    @Test
    public void testCheckerLackTwoMasterMessenger() {
        MessengerInstanceManager.MessengerChecker checker = messengerInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto("mha1_dc2_dalcluster.mha1_dc2", "db1", "127.0.2.1", 8080, false, "10.1.1.1"),
                getApplierInfoDto("mha1_dc2_dalcluster.mha1_dc2", "db1", "127.0.2.2", 8080, false, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(instanceStateController.getMessengerInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeMessenger(any(), any(), anyBoolean());
        verify(instanceStateController, times(1)).addMessenger(any(), any());
        verify(instanceStateController, never()).registerMessenger(any(), any());
    }



    private void init() {
        DbCluster dbCluster1 = new DbCluster("mha1_dc1_dalcluster.mha1_dc1")
                .addMessenger(new Messenger().setIp("127.0.1.1").setPort(8080).setMaster(true).setApplyMode(ApplyMode.mq.getType()).setIncludedDbs(null))
                .addMessenger(new Messenger().setIp("127.0.1.2").setPort(8080).setMaster(false).setApplyMode(ApplyMode.mq.getType()).setIncludedDbs(null));

        DbCluster dbCluster2 = new DbCluster("mha1_dc2_dalcluster.mha1_dc2")
                .addMessenger(new Messenger().setIp("127.0.2.1").setPort(8080).setMaster(true).setApplyMode(ApplyMode.db_mq.getType()).setIncludedDbs("db1"))
                .addMessenger(new Messenger().setIp("127.0.2.2").setPort(8080).setMaster(false).setApplyMode(ApplyMode.db_mq.getType()).setIncludedDbs("db1"));
        Drc mock = new Drc().addDc(new Dc("dc1").addDbCluster(dbCluster1))
                .addDc(new Dc("dc2").addDbCluster(dbCluster2));
        List<DbCluster> dbClusters = mock.getDcs().values().stream().flatMap(e -> e.getDbClusters().values().stream()).collect(Collectors.toList());
        Map<String, Map<String, List<Messenger>>> mockMap = new HashMap<>();
        for (DbCluster dbCluster : dbClusters) {
            mockMap.put(dbCluster.getId(), dbCluster.getMessengers().stream().collect(Collectors.groupingBy(NameUtils::getMessengerDbName)));
        }

        when(clusterManagerConfig.getCheckMaxTime()).thenReturn(3*1000);
        when(currentMetaManager.getAllMetaMessengers()).thenReturn(mockMap);
        when(currentMetaManager.getActiveReplicator(ArgumentMatchers.anyString())).thenReturn(new Replicator().setIp("10.1.1.1"));
        when(clusterManagerConfig.getPeriodCheckSwitch()).thenReturn(true);
        when(clusterManagerConfig.getPeriodCorrectSwitch()).thenReturn(true);
        when(currentMetaManager.hasCluster("mha1_dc1_dalcluster.mha1_dc1")).thenReturn(true);
        when(currentMetaManager.hasCluster("mha1_dc2_dalcluster.mha1_dc2")).thenReturn(true);
    }

    private static ApplierInfoDto getApplierInfoDto(String clusterId, String ip, int port, boolean master, String replicatorIp) {
        return getApplierInfoDto(clusterId, DRC_MQ, ip, port, master, replicatorIp);
    }

    private static ApplierInfoDto getApplierInfoDto(String clusterId, String db, String ip, int port, boolean master, String replicatorIp) {
        ApplierInfoDto dto = new ApplierInfoDto();
        dto.setMaster(master);
        dto.setReplicatorIp(replicatorIp);
        dto.setIp(ip);
        dto.setPort(port);
        dto.setRegistryKey(NameUtils.getMessengerRegisterKey(clusterId, db));
        return dto;
    }
}
