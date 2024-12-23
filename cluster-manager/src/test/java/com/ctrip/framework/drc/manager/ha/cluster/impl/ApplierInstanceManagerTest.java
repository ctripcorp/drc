package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierInfoDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.service.inquirer.BatchInfoInquirer;
import com.ctrip.framework.drc.core.utils.NameUtils;
import com.ctrip.framework.drc.manager.enums.ServerStateEnum;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.*;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Created by jixinwang on 2023/8/31
 */
public class ApplierInstanceManagerTest {

    @InjectMocks
    public ApplierInstanceManager applierInstanceManager;

    @Mock
    protected InstanceStateController instanceStateController;

    @Mock
    protected CurrentMetaManager currentMetaManager;

    @Mock
    protected ClusterManagerConfig clusterManagerConfig;

    @Mock
    private DefaultClusterManagers clusterServers;

    @Mock
    private ClusterServerStateManager clusterServerStateManager;

    @Mock
    BatchInfoInquirer batchInfoInquirer;



    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(clusterServerStateManager.getServerState()).thenReturn(ServerStateEnum.NORMAL);
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
        Applier applier = new Applier();
        applier.setIp("127.0.0.1");
        applier.setPort(8080);
        applier.setApplyMode(ApplyMode.transaction_table.getType());
        current.addApplier(applier);

        DbCluster future = new DbCluster();
        future.setDbs(dbs);
        future.setId(clusterId);
        Applier applier2 = new Applier();
        applier2.setProperties("test_property");
        applier2.setIp("127.0.0.1");
        applier2.setPort(8080);
        applier2.setApplyMode(ApplyMode.transaction_table.getType());
        future.addApplier(applier2);

        ClusterComparator comparator = new ClusterComparator(current, future);
        comparator.compare();

        Mockito.when(currentMetaManager.getActiveApplier(Mockito.anyString(), Mockito.anyString())).thenReturn(applier);
        Mockito.when(clusterManagerConfig.checkApplierProperty()).thenReturn(true);

        applierInstanceManager.handleClusterModified(comparator);

        Mockito.verify(instanceStateController, times(1)).applierPropertyChange(clusterId, applier2);
    }


    @Test
    public void testChecker() {
        ApplierInstanceManager.ApplierChecker checker = applierInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.2", 8080, false, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(batchInfoInquirer.getApplierInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeApplier(any(), any(), anyBoolean());
        verify(instanceStateController, never()).addApplier(any(), any());
        verify(instanceStateController, never()).registerApplier(any(), any());
    }


    @Test
    public void testCheckerRegister() {
        ApplierInstanceManager.ApplierChecker checker = applierInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList();
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(batchInfoInquirer.getApplierInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeApplier(any(), any(), anyBoolean());
        verify(instanceStateController, never()).addApplier(any(), any());
        verify(instanceStateController, times(4)).registerApplier(any(), any());
    }

    @Test
    public void testCheckerReshardSkipRegister() {
        ApplierInstanceManager.ApplierChecker checker = applierInstanceManager.getChecker();

        init();
        when(clusterServers.getLastChildEventTime()).thenReturn(System.currentTimeMillis()+100000);

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList();
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(batchInfoInquirer.getApplierInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeApplier(any(), any(), anyBoolean());
        verify(instanceStateController, never()).addApplier(any(), any());
        verify(instanceStateController, times(0)).registerApplier(any(), any());
        when(clusterServers.getLastChildEventTime()).thenReturn(0L);
    }

    @Test
    public void testCheckerBothMhaDbApplier() {
        ApplierInstanceManager.ApplierChecker checker = applierInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1"), "127.0.2.1", 8080, true, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(batchInfoInquirer.getApplierInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, times(1)).removeApplier(any(), any(), anyBoolean());
        verify(instanceStateController, never()).addApplier(any(), any());
        verify(instanceStateController, never()).registerApplier(any(), any());
    }

    @Test
    public void testCheckerDbMaster() {
        ApplierInstanceManager.ApplierChecker checker = applierInstanceManager.getChecker();

        init();

        ApplierInfoDto applierInfoDto = getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.1", 8080, true, "10.1.1.1");
        DBInfo dbInfo = new DBInfo();
        dbInfo.setUuid("aaaa-bbbb-cccc");
        dbInfo.setIp("123");
        dbInfo.setPort(55111);
        applierInfoDto.setDbInfo(dbInfo);
        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                applierInfoDto,
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.2", 8080, false, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(batchInfoInquirer.getApplierInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeApplier(any(), any(), anyBoolean());
        verify(instanceStateController, times(1)).addApplier(any(), any());
        verify(instanceStateController, never()).registerApplier(any(), any());
    }

    @Test
    public void testCheckerDbMaster2() {
        ApplierInstanceManager.ApplierChecker checker = applierInstanceManager.getChecker();

        init();

        ApplierInfoDto applierInfoDto = getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.1", 8080, true, "10.1.1.1");
        DBInfo dbInfo = new DBInfo();
        dbInfo.setUuid("aaaa-bbbb-cccc");
        dbInfo.setIp("db1_ip_wrong");
        dbInfo.setPort(55111);
        applierInfoDto.setDbInfo(dbInfo);
        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                applierInfoDto,
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.2", 8080, false, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(batchInfoInquirer.getApplierInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeApplier(any(), any(), anyBoolean());
        verify(instanceStateController, times(1)).addApplier(any(), any());
        verify(instanceStateController, never()).registerApplier(any(), any());
    }

    @Test
    public void testCheckerDbMasterOK() {
        ApplierInstanceManager.ApplierChecker checker = applierInstanceManager.getChecker();

        init();

        ApplierInfoDto applierInfoDto = getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.1", 8080, true, "10.1.1.1");
        DBInfo dbInfo = new DBInfo();
        dbInfo.setUuid("aaaa-bbbb-cccc");
        dbInfo.setIp("db1_ip_right");
        dbInfo.setPort(55111);
        applierInfoDto.setDbInfo(dbInfo);
        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                applierInfoDto,
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.2", 8080, false, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(batchInfoInquirer.getApplierInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeApplier(any(), any(), anyBoolean());
        verify(instanceStateController, never()).addApplier(any(), any());
        verify(instanceStateController, never()).registerApplier(any(), any());
    }

    @Test
    public void testCheckerMissApplierIp() {
        ApplierInstanceManager.ApplierChecker checker = applierInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.3", 8080, false, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.2", 8080, false, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2", "127.0.1.3");
        when(batchInfoInquirer.getApplierInfo(anyList())).thenAnswer(e -> {
            List<Instance> appliers = e.getArgument(0, List.class);
            Set<String> ips = appliers.stream().map(applier -> applier.getIp()).collect(Collectors.toSet());
            return Pair.from(validIps.stream().filter(ips::contains).collect(Collectors.toList()), instanceList.stream().filter(applier -> ips.contains(applier.getIp())).collect(Collectors.toList()));
        });
        when(currentMetaManager.getAllApplierOrMessengerInstances()).thenReturn(Sets.newHashSet(
                SimpleInstance.from("127.0.1.1", 8080),
                SimpleInstance.from("127.0.1.2", 8080),
                SimpleInstance.from("127.0.1.3", 8080),
                SimpleInstance.from("127.0.2.1", 8080),
                SimpleInstance.from("127.0.2.2", 8080)
        ));
        checker.run();
        verify(instanceStateController, times(1)).removeApplier(any(), any(), anyBoolean());
        verify(instanceStateController, never()).addApplier(any(), any());
        verify(instanceStateController, never()).registerApplier(any(), any());
    }

    @Test
    public void testCheckerFailHttp() {
        ApplierInstanceManager.ApplierChecker checker = applierInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.1", 8080, true, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1");
        when(batchInfoInquirer.getApplierInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeApplier(any(), any(), anyBoolean());
        verify(instanceStateController, never()).addApplier(any(), any());
        verify(instanceStateController, never()).registerApplier(any(), any());

    }

    @Test
    public void testCheckerFailHttp2() {
        ApplierInstanceManager.ApplierChecker checker = applierInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.1", 8080, true, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.3");
        when(batchInfoInquirer.getApplierInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeApplier(any(), any(), anyBoolean());
        verify(instanceStateController, never()).addApplier(any(), any());
        verify(instanceStateController, never()).registerApplier(any(), any());
    }


    @Test
    public void testCheckerWrongMaster() {
        ApplierInstanceManager.ApplierChecker checker = applierInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.1", 8080, false, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.2", 8080, true, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(batchInfoInquirer.getApplierInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeApplier(any(), any(), anyBoolean());
        verify(instanceStateController, times(1)).addApplier(any(), any());
        verify(instanceStateController, never()).registerApplier(any(), any());
    }

    @Test
    public void testCheckerTwoMaster() {
        init();
        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.2", 8080, true, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(batchInfoInquirer.getApplierInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));

        ApplierInstanceManager.ApplierChecker checker = applierInstanceManager.getChecker();
        checker.run();

        verify(instanceStateController, never()).removeApplier(any(), any(), anyBoolean());
        verify(instanceStateController, times(1)).addApplier(any(), any());
        verify(instanceStateController, never()).registerApplier(any(), any());
    }

    @Test
    public void testCheckerWrongReplicator() {
        ApplierInstanceManager.ApplierChecker checker = applierInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.1", 8080, true, "10.1.1.2"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.2", 8080, false, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(batchInfoInquirer.getApplierInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeApplier(any(), any(), anyBoolean());
        verify(instanceStateController, times(1)).addApplier(any(), any());
        verify(instanceStateController, never()).registerApplier(any(), any());
    }


    @Test
    public void testCheckerRedundantApplier() {
        ApplierInstanceManager.ApplierChecker checker = applierInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.3", 8080, false, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2", "127.0.2.3");
        when(batchInfoInquirer.getApplierInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, times(1)).removeApplier(any(), any(), anyBoolean());
        verify(instanceStateController, never()).addApplier(any(), any());
        verify(instanceStateController, never()).registerApplier(any(), any());
    }

    @Test
    public void testCheckerLackApplier() {
        ApplierInstanceManager.ApplierChecker checker = applierInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.1", 8080, true, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(batchInfoInquirer.getApplierInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeApplier(any(), any(), anyBoolean());
        verify(instanceStateController, times(1)).registerApplier(eq("mha1_dc2_dalcluster.mha1_dc2"), any());
    }


    @Test
    public void testCheckerIgnoreUninterestedCluster() {
        ApplierInstanceManager.ApplierChecker checker = applierInstanceManager.getChecker();

        init();

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "other.thter", "other_mha", "db1"), "127.0.2.2", 8080, false, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(batchInfoInquirer.getApplierInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        checker.run();
        verify(instanceStateController, never()).removeApplier(any(), any(), anyBoolean());
        verify(instanceStateController, never()).addApplier(any(), any());
        verify(instanceStateController, never()).registerApplier(any(), any());
    }


    private void init() {
        Db db = new Db().setMaster(true).setIp("db1_ip_right").setPort(55111).setUuid("aaaa-bbbb-cccc");
        DbCluster dbCluster1 = new DbCluster("mha1_dc1_dalcluster.mha1_dc1")
                .setDbs(new Dbs().addDb(new Db().setMaster(true).setIp("db1_ip_wrong").setPort(55111).setUuid("aaaa-bbbb-cccc")))
                .addApplier(new Applier().setIp("127.0.1.1").setPort(8080).setMaster(true).setTargetMhaName("mha1_dc2").setTargetName("mha1_dc2_dalcluster").setApplyMode(ApplyMode.transaction_table.getType()).setIncludedDbs(null))
                .addApplier(new Applier().setIp("127.0.1.2").setPort(8080).setMaster(false).setTargetMhaName("mha1_dc2").setTargetName("mha1_dc2_dalcluster").setApplyMode(ApplyMode.transaction_table.getType()).setIncludedDbs(null));

        DbCluster dbCluster2 = new DbCluster("mha1_dc2_dalcluster.mha1_dc2")
                .addApplier(new Applier().setIp("127.0.2.1").setPort(8080).setMaster(true).setTargetMhaName("mha1_dc1").setTargetName("mha1_dc1_dalcluster").setApplyMode(ApplyMode.db_transaction_table.getType()).setIncludedDbs("db1"))
                .addApplier(new Applier().setIp("127.0.2.2").setPort(8080).setMaster(false).setTargetMhaName("mha1_dc1").setTargetName("mha1_dc1_dalcluster").setApplyMode(ApplyMode.db_transaction_table.getType()).setIncludedDbs("db1"));
        Drc mock = new Drc().addDc(new Dc("dc1").addDbCluster(dbCluster1))
                .addDc(new Dc("dc2").addDbCluster(dbCluster2));
        List<DbCluster> dbClusters = mock.getDcs().values().stream().flatMap(e -> e.getDbClusters().values().stream()).collect(Collectors.toList());
        Map<String, Map<String, List<Applier>>> mockMap = new HashMap<>();
        for (DbCluster dbCluster : dbClusters) {
            mockMap.put(dbCluster.getId(), dbCluster.getAppliers().stream().collect(Collectors.groupingBy(NameUtils::getApplierBackupRegisterKey)));
        }

        when(currentMetaManager.getAllMetaAppliers()).thenReturn(mockMap);
        when(currentMetaManager.getApplierMaster(any(), anyString())).thenReturn(Pair.from("10.1.1.1", 8080));
        when(clusterManagerConfig.getPeriodCheckSwitch()).thenReturn(true);
        when(clusterManagerConfig.getPeriodCorrectSwitch()).thenReturn(true);
        when(clusterManagerConfig.getCheckMaxTime()).thenReturn(3 * 1000);
        when(currentMetaManager.getCluster("mha1_dc1_dalcluster.mha1_dc1")).thenReturn(dbCluster1);
        when(currentMetaManager.getMySQLMaster("mha1_dc1_dalcluster.mha1_dc1")).thenReturn(new DefaultEndPoint(db.getIp(), db.getPort()));
        when(currentMetaManager.hasCluster("mha1_dc1_dalcluster.mha1_dc1")).thenReturn(true);
        when(currentMetaManager.hasCluster("mha1_dc2_dalcluster.mha1_dc2")).thenReturn(true);
    }

    private static ApplierInfoDto getApplierInfoDto(String registryKey, String ip, int port, boolean master, String replicatorIp) {
        ApplierInfoDto dto = new ApplierInfoDto();
        dto.setMaster(master);
        dto.setReplicatorIp(replicatorIp);
        dto.setIp(ip);
        dto.setPort(port);
        dto.setRegistryKey(registryKey);
        return dto;
    }
}