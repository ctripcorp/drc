package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorInfoDto;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * @author: yongnian
 * @create: 2024/5/24 15:03
 */
public class ReplicatorCheckerTest {
    public static final String DB_1_IP = "db_1_ip";
    public static final String DB_2_IP = "db_2_ip";
    public static final int DB_PORT = 55111;
    public static final int REPLICATOR_PORT = 8080;
    @InjectMocks
    ReplicatorInstanceManager replicatorInstanceManager = new ReplicatorInstanceManager();

    @Mock
    protected InstanceStateController instanceStateController;

    @Mock
    protected CurrentMetaManager currentMetaManager;

    @Mock
    protected ClusterManagerConfig clusterManagerConfig;

    @Mock
    private DefaultClusterManagers clusterServers;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testChecker() {
        ReplicatorInstanceManager.ReplicatorChecker checker = replicatorInstanceManager.getChecker();

        init();

        ArrayList<ReplicatorInfoDto> instanceList = Lists.newArrayList(
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", REPLICATOR_PORT, true, DB_1_IP),
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", REPLICATOR_PORT, false, "127.0.1.1"),
                getReplicatorInfoDto("mha1_dc2_dalcluster.mha1_dc2", "127.0.2.1", REPLICATOR_PORT, true, DB_2_IP),
                getReplicatorInfoDto("mha1_dc2_dalcluster.mha1_dc2", "127.0.2.2", REPLICATOR_PORT, false, "127.0.2.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(instanceStateController.getReplicatorInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeReplicator(any(), any());
        verify(instanceStateController, never()).addReplicator(any(), any());
        verify(instanceStateController, never()).registerReplicator(any(), any());
    }

    @Test
    public void testCheckerRegister() {
        ReplicatorInstanceManager.ReplicatorChecker checker = replicatorInstanceManager.getChecker();

        init();

        ArrayList<ReplicatorInfoDto> instanceList = Lists.newArrayList();
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.3");
        when(instanceStateController.getReplicatorInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeReplicator(any(), any());
        verify(instanceStateController, times(3)).registerReplicator(any(), any());
    }

    @Test
    public void testCheckerNoCorrect() {
        ReplicatorInstanceManager.ReplicatorChecker checker = replicatorInstanceManager.getChecker();

        init();

        when(clusterManagerConfig.getPeriodCorrectSwitch()).thenReturn(false);
        ArrayList<ReplicatorInfoDto> instanceList = Lists.newArrayList();
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.3");
        when(instanceStateController.getReplicatorInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeReplicator(any(), any());
        verify(instanceStateController, never()).addReplicator(any(), any());
        verify(instanceStateController, never()).registerReplicator(any(), any());

    }

    @Test
    public void testCheckerMissReplicatorIp() {
        ReplicatorInstanceManager.ReplicatorChecker checker = replicatorInstanceManager.getChecker();

        init();

        ArrayList<ReplicatorInfoDto> instanceList = Lists.newArrayList(
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", REPLICATOR_PORT, true, DB_1_IP),
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", REPLICATOR_PORT, false, "127.0.1.1"),
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.3", REPLICATOR_PORT, false, "127.0.1.1"),
                getReplicatorInfoDto("mha1_dc2_dalcluster.mha1_dc2", "127.0.2.1", REPLICATOR_PORT, true, DB_2_IP),
                getReplicatorInfoDto("mha1_dc2_dalcluster.mha1_dc2", "127.0.2.2", REPLICATOR_PORT, false, "127.0.2.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2", "127.0.1.3");
        when(instanceStateController.getReplicatorInfo(anyList())).thenAnswer(e -> {
            List<Instance> replicators = e.getArgument(0, List.class);
            Set<String> ips = replicators.stream().map(Instance::getIp).collect(Collectors.toSet());
            return Pair.from(validIps.stream().filter(ips::contains).collect(Collectors.toList()), instanceList.stream().filter(applier -> ips.contains(applier.getIp())).collect(Collectors.toList()));
        });
        when(currentMetaManager.getAllReplicatorInstances()).thenReturn(Sets.newHashSet(
                SimpleInstance.from("127.0.1.1", REPLICATOR_PORT),
                SimpleInstance.from("127.0.1.2", REPLICATOR_PORT),
                SimpleInstance.from("127.0.1.3", REPLICATOR_PORT),
                SimpleInstance.from("127.0.2.1", REPLICATOR_PORT),
                SimpleInstance.from("127.0.2.2", REPLICATOR_PORT)
        ));
        checker.run();
        verify(instanceStateController, never()).removeReplicator(any(), any());
        verify(instanceStateController, never()).addReplicator(any(), any());
        verify(instanceStateController, never()).registerReplicator(any(), any());
    }

    @Test
    public void testCheckerFailHttp() {
        ReplicatorInstanceManager.ReplicatorChecker checker = replicatorInstanceManager.getChecker();

        init();

        ArrayList<ReplicatorInfoDto> instanceList = Lists.newArrayList(
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", REPLICATOR_PORT, true, DB_1_IP),
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", REPLICATOR_PORT, false, "127.0.1.1"),
                getReplicatorInfoDto("mha1_dc2_dalcluster.mha1_dc2", "127.0.2.1", REPLICATOR_PORT, true, DB_2_IP)
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1");
        when(instanceStateController.getReplicatorInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeReplicator(any(), any());
        verify(instanceStateController, never()).addReplicator(any(), any());
        verify(instanceStateController, never()).registerReplicator(any(), any());

    }

    @Test
    public void testCheckerFailHttp2() {
        ReplicatorInstanceManager.ReplicatorChecker checker = replicatorInstanceManager.getChecker();

        init();

        ArrayList<ReplicatorInfoDto> instanceList = Lists.newArrayList(
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", REPLICATOR_PORT, true, DB_1_IP),
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", REPLICATOR_PORT, false, "127.0.1.1"),
                getReplicatorInfoDto("mha1_dc2_dalcluster.mha1_dc2", "127.0.2.1", REPLICATOR_PORT, true, DB_2_IP)
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.3");
        when(instanceStateController.getReplicatorInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeReplicator(any(), any());
        verify(instanceStateController, never()).addReplicator(any(), any());
        verify(instanceStateController, never()).registerReplicator(any(), any());
    }


    @Test
    public void testCheckerWrongMaster() {
        ReplicatorInstanceManager.ReplicatorChecker checker = replicatorInstanceManager.getChecker();

        init();

        ArrayList<ReplicatorInfoDto> instanceList = Lists.newArrayList(
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", REPLICATOR_PORT, true, DB_1_IP),
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", REPLICATOR_PORT, false, "127.0.1.1"),
                getReplicatorInfoDto("mha1_dc2_dalcluster.mha1_dc2", "127.0.2.1", REPLICATOR_PORT, false, "127.0.2.1"),
                getReplicatorInfoDto("mha1_dc2_dalcluster.mha1_dc2", "127.0.2.2", REPLICATOR_PORT, true, DB_2_IP)
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(instanceStateController.getReplicatorInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeReplicator(any(), any());
        verify(instanceStateController, times(1)).addReplicator(any(), any());
        verify(instanceStateController, never()).registerReplicator(any(), any());
    }

    @Test
    public void testCheckerTwoMaster() {
        init();
        ArrayList<ReplicatorInfoDto> instanceList = Lists.newArrayList(
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", REPLICATOR_PORT, true, DB_1_IP),
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", REPLICATOR_PORT, false, "127.0.1.1"),
                getReplicatorInfoDto("mha1_dc2_dalcluster.mha1_dc2", "127.0.2.1", REPLICATOR_PORT, true, DB_2_IP),
                getReplicatorInfoDto("mha1_dc2_dalcluster.mha1_dc2", "127.0.2.2", REPLICATOR_PORT, true, DB_2_IP)
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(instanceStateController.getReplicatorInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));

        ReplicatorInstanceManager.ReplicatorChecker checker = replicatorInstanceManager.getChecker();
        checker.run();

        verify(instanceStateController, never()).removeReplicator(any(), any());
        verify(instanceStateController, times(1)).addReplicator(any(), any());
        verify(instanceStateController, never()).registerReplicator(any(), any());

    }

    @Test
    public void testCheckerWrongUpStreamIp() {
        ReplicatorInstanceManager.ReplicatorChecker checker = replicatorInstanceManager.getChecker();

        init();

        ArrayList<ReplicatorInfoDto> instanceList = Lists.newArrayList(
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", REPLICATOR_PORT, true, DB_1_IP),
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", REPLICATOR_PORT, false, "127.0.2.1"),
                getReplicatorInfoDto("mha1_dc2_dalcluster.mha1_dc2", "127.0.2.1", REPLICATOR_PORT, true, DB_2_IP),
                getReplicatorInfoDto("mha1_dc2_dalcluster.mha1_dc2", "127.0.2.2", REPLICATOR_PORT, false, "127.0.2.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(instanceStateController.getReplicatorInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeReplicator(any(), any());
        verify(instanceStateController, times(1)).addReplicator(any(), any());
        verify(instanceStateController, never()).registerReplicator(any(), any());

    }


    @Test
    public void testCheckerRedundantReplicator() {
        ReplicatorInstanceManager.ReplicatorChecker checker = replicatorInstanceManager.getChecker();

        init();

        ArrayList<ReplicatorInfoDto> instanceList = Lists.newArrayList(
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", REPLICATOR_PORT, true, DB_1_IP),
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", REPLICATOR_PORT, false, "127.0.1.1"),
                getReplicatorInfoDto("mha1_dc2_dalcluster.mha1_dc2", "127.0.2.1", REPLICATOR_PORT, true, DB_2_IP),
                getReplicatorInfoDto("mha1_dc2_dalcluster.mha1_dc2", "127.0.2.2", REPLICATOR_PORT, false, "127.0.2.1"),
                getReplicatorInfoDto("mha1_dc2_dalcluster.mha1_dc2", "127.0.2.3", REPLICATOR_PORT, false, "127.0.2.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2", "127.0.2.3");
        when(instanceStateController.getReplicatorInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeReplicator(any(), any());
        verify(instanceStateController, never()).addReplicator(any(), any());
        verify(instanceStateController, never()).registerReplicator(any(), any());
    }

    @Test
    public void testCheckerLackReplicator() {
        ReplicatorInstanceManager.ReplicatorChecker checker = replicatorInstanceManager.getChecker();

        init();

        ArrayList<ReplicatorInfoDto> instanceList = Lists.newArrayList(
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", REPLICATOR_PORT, true, DB_1_IP),
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", REPLICATOR_PORT, false, "127.0.1.1"),
                getReplicatorInfoDto("mha1_dc2_dalcluster.mha1_dc2", "127.0.2.1", REPLICATOR_PORT, true, DB_2_IP)
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(instanceStateController.getReplicatorInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeReplicator(any(), any());
        verify(instanceStateController, times(0)).addReplicator(any(), any());
        verify(instanceStateController, times(1)).registerReplicator(any(), any());
    }


    @Test
    public void testCheckerIgnoreUninterestedCluster() {
        ReplicatorInstanceManager.ReplicatorChecker checker = replicatorInstanceManager.getChecker();

        init();

        ArrayList<ReplicatorInfoDto> instanceList = Lists.newArrayList(
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", REPLICATOR_PORT, true, DB_1_IP),
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", REPLICATOR_PORT, false, "127.0.1.1"),
                getReplicatorInfoDto("mha1_dc2_dalcluster.mha1_dc2", "127.0.2.1", REPLICATOR_PORT, true, DB_2_IP),
                getReplicatorInfoDto("mha1_dc2_dalcluster.mha1_dc2", "127.0.2.2", REPLICATOR_PORT, false, "127.0.2.1"),
                getReplicatorInfoDto(String.join(".", "other.thter", "other_mha", "db1"), "127.0.2.2", REPLICATOR_PORT, false, "127.0.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(instanceStateController.getReplicatorInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeReplicator(any(), any());
        verify(instanceStateController, never()).addReplicator(any(), any());
        verify(instanceStateController, never()).registerReplicator(any(), any());
    }


    private void init() {
        DbCluster dbCluster1 = new DbCluster("mha1_dc1_dalcluster.mha1_dc1")
                .addReplicator(new Replicator().setIp("127.0.1.1").setPort(REPLICATOR_PORT).setMaster(true))
                .addReplicator(new Replicator().setIp("127.0.1.2").setPort(REPLICATOR_PORT).setMaster(false));
        dbCluster1.setDbs(new Dbs().addDb(new Db().setIp(DB_1_IP).setPort(DB_PORT).setMaster(true)));

        DbCluster dbCluster2 = new DbCluster("mha1_dc2_dalcluster.mha1_dc2")
                .addReplicator(new Replicator().setIp("127.0.2.1").setPort(REPLICATOR_PORT).setMaster(true))
                .addReplicator(new Replicator().setIp("127.0.2.2").setPort(REPLICATOR_PORT).setMaster(false));
        dbCluster2.setDbs(new Dbs().addDb(new Db().setIp(DB_2_IP).setPort(DB_PORT).setMaster(true)));

        Drc mock = new Drc().addDc(new Dc("dc1").addDbCluster(dbCluster1))
                .addDc(new Dc("dc2").addDbCluster(dbCluster2));
        List<DbCluster> dbClusters = mock.getDcs().values().stream().flatMap(e -> e.getDbClusters().values().stream()).collect(Collectors.toList());
        Map<String, List<Replicator>> mockMap = new HashMap<>();
        for (DbCluster dbCluster : dbClusters) {
            mockMap.put(dbCluster.getId(), dbCluster.getReplicators());
        }
        Map<String, Replicator> masterMap = new HashMap<>();
        Map<String, DbCluster> clusterMap = new HashMap<>();
        dbCluster1.getReplicators().stream().filter(Replicator::getMaster).forEach(e -> masterMap.put(dbCluster1.getId(), e));
        dbCluster2.getReplicators().stream().filter(Replicator::getMaster).forEach(e -> masterMap.put(dbCluster2.getId(), e));
        clusterMap.put(dbCluster1.getId(), dbCluster1);
        clusterMap.put(dbCluster2.getId(), dbCluster2);
        when(currentMetaManager.getAllMetaReplicator()).thenReturn(mockMap);
        when(currentMetaManager.getActiveReplicator(anyString())).thenAnswer(e -> {
            String clusterId = e.getArgument(0);
            return masterMap.get(clusterId);
        });
        when(currentMetaManager.getCluster(anyString())).thenAnswer(e -> {
            String clusterId = e.getArgument(0);
            return clusterMap.get(clusterId);
        });
        when(clusterManagerConfig.getCheckMaxTime()).thenReturn(3 * 1000);
        when(clusterManagerConfig.getPeriodCheckSwitch()).thenReturn(true);
        when(clusterManagerConfig.getPeriodCorrectSwitch()).thenReturn(true);
        when(currentMetaManager.getMySQLMaster("mha1_dc1_dalcluster.mha1_dc1")).thenReturn(new DefaultEndPoint(DB_1_IP, DB_PORT));
        when(currentMetaManager.getMySQLMaster("mha1_dc2_dalcluster.mha1_dc2")).thenReturn(new DefaultEndPoint(DB_2_IP, DB_PORT));
        when(currentMetaManager.hasCluster("mha1_dc1_dalcluster.mha1_dc1")).thenReturn(true);
        when(currentMetaManager.hasCluster("mha1_dc2_dalcluster.mha1_dc2")).thenReturn(true);
    }

    private static ReplicatorInfoDto getReplicatorInfoDto(String registryKey, String ip, int port, boolean master, String upstreamIp) {
        ReplicatorInfoDto dto = new ReplicatorInfoDto();
        dto.setMaster(master);
        dto.setUpstreamMasterIp(upstreamIp);
        dto.setIp(ip);
        dto.setPort(port);
        dto.setRegistryKey(registryKey);
        return dto;
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme