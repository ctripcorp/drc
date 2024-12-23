package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.core.entity.Instance;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierInfoDto;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorInfoDto;
import com.ctrip.framework.drc.core.service.inquirer.BatchInfoInquirer;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

/**
 * @author yongnian
 * @create 2024/12/23 19:34
 */
public class RemoteResourceServiceImplTest {

    @InjectMocks
    RemoteResourceServiceImpl remoteResourceServiceImpl;
    @Mock
    BatchInfoInquirer batchInfoInquirer;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testQueryApplier() {
        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc1_dalcluster.mha1_dc1", "mha1_dc2"), "127.0.1.2", 8080, false, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto(String.join(".", "mha1_dc2_dalcluster.mha1_dc2", "mha1_dc1", "db1"), "127.0.2.2", 8080, false, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(batchInfoInquirer.getApplierInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        Map<String, List<Instance>> currentDbApplierInstances = remoteResourceServiceImpl.getCurrentDbApplierInstances("mha1_dc1", "mha1_dc2", validIps);
        Assert.assertEquals(1, currentDbApplierInstances.size());
        Assert.assertEquals("db1", currentDbApplierInstances.keySet().iterator().next());
        Assert.assertEquals(2, currentDbApplierInstances.values().iterator().next().size());
    }

    @Test
    public void testQueryReplicator() {
        int REPLICATOR_PORT = 8080;
        ArrayList<ReplicatorInfoDto> instanceList = Lists.newArrayList(
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.1", REPLICATOR_PORT, true, "DB_1_IP"),
                getReplicatorInfoDto("mha1_dc1_dalcluster.mha1_dc1", "127.0.1.2", REPLICATOR_PORT, false, "127.0.1.1"),
                getReplicatorInfoDto("mha1_dc2_dalcluster.mha1_dc2", "127.0.2.1", REPLICATOR_PORT, true, "DB_2_IP"),
                getReplicatorInfoDto("mha1_dc2_dalcluster.mha1_dc2", "127.0.2.2", REPLICATOR_PORT, false, "127.0.2.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(batchInfoInquirer.getReplicatorInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));
        List<Instance> currentDbApplierInstances = remoteResourceServiceImpl.getCurrentReplicatorInstance("mha1_dc1", validIps);
        Assert.assertEquals(2, currentDbApplierInstances.size());
        Assert.assertTrue(currentDbApplierInstances.stream().anyMatch(e -> e.getIp().equals("127.0.1.1") && e.getMaster()));
        Assert.assertTrue(currentDbApplierInstances.stream().anyMatch(e -> e.getIp().equals("127.0.1.2") && !e.getMaster()));

    }

    @Test
    public void testQueryMessenger() {
        int REPLICATOR_PORT = 8080;

        ArrayList<ApplierInfoDto> instanceList = Lists.newArrayList(
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1._drc_mq", "127.0.1.1", 8080, true, "10.1.1.1"),
                getApplierInfoDto("mha1_dc1_dalcluster.mha1_dc1._drc_mq", "127.0.1.2", 8080, false, "10.1.1.1")
        );
        List<String> validIps = Lists.newArrayList("127.0.1.1", "127.0.1.2", "127.0.2.1", "127.0.2.2");
        when(batchInfoInquirer.getMessengerInfo(anyList())).thenReturn(Pair.from(validIps, instanceList));


        List<Instance> currentDbApplierInstances = remoteResourceServiceImpl.getCurrentMessengerInstance("mha1_dc1", validIps);
        Assert.assertEquals(2, currentDbApplierInstances.size());
        Assert.assertTrue(currentDbApplierInstances.stream().anyMatch(e -> e.getIp().equals("127.0.1.1") && e.getMaster()));
        Assert.assertTrue(currentDbApplierInstances.stream().anyMatch(e -> e.getIp().equals("127.0.1.2") && !e.getMaster()));

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