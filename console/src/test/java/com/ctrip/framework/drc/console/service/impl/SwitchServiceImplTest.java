package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.KafkaDelayMonitorServer;
import com.ctrip.framework.drc.console.monitor.delay.task.ListenReplicatorTask;
import com.ctrip.framework.drc.console.service.broadcast.HttpNotificationBroadCast;
import com.ctrip.framework.drc.console.service.v2.DbMetaCorrectService;
import com.ctrip.framework.drc.core.server.config.console.dto.ClusterConfigDto;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.web.bind.annotation.RequestMethod;

import java.util.HashMap;
import java.util.Map;

import static com.ctrip.framework.drc.console.monitor.MockTest.times;
import static com.ctrip.framework.drc.console.utils.UTConstants.*;
import static org.mockito.Mockito.verify;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-24
 */
public class SwitchServiceImplTest {

    @InjectMocks
    private SwitchServiceImpl switchService;

    @Mock
    private DefaultCurrentMetaManager currentMetaManager;

    @Mock
    private DbMetaCorrectService dbMetaCorrectService;

    @Mock
    private ListenReplicatorTask listenReplicatorTask;

    @Mock
    private HttpNotificationBroadCast broadCast;

    @Mock
    private KafkaDelayMonitorServer kafkaDelayMonitorServer;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        Mockito.doNothing().when(currentMetaManager).updateMasterMySQL(Mockito.anyString(), Mockito.any());
        Mockito.doNothing().when(broadCast).broadcastWithRetry(Mockito.anyString(), Mockito.any(RequestMethod.class), Mockito.anyString(), Mockito.anyInt());
    }

    @Test
    public void testBatchSwitchUpdateDb() throws Exception {
        ClusterConfigDto clusterConfigDto = new ClusterConfigDto();
        Map<String, String> map = new HashMap<>();
        map.put(CLUSTER_ID1, IP1 + ":" + MYSQL_PORT);
        clusterConfigDto.setClusterMap(map);
        clusterConfigDto.setFirstHand(true);
        Mockito.doNothing().when(dbMetaCorrectService).batchMhaMasterDbChange(Mockito.anyList());
        switchService.switchUpdateDb(clusterConfigDto);
        Thread.sleep(1000);
        verify(currentMetaManager, times(1)).updateMasterMySQL(Mockito.anyString(), Mockito.any());
        verify(dbMetaCorrectService, times(1)).batchMhaMasterDbChange(Mockito.anyList());
        verify(broadCast, times(1)).broadcastWithRetry(Mockito.anyString(), Mockito.any(RequestMethod.class), Mockito.anyString(), Mockito.anyInt());
    }


    @Test
    public void testBatchSwitchListenReplicator() throws InterruptedException {
        ClusterConfigDto clusterConfigDto = new ClusterConfigDto();
        Map<String, String> map = new HashMap<>();
        map.put(CLUSTER_ID1, IP1 + ":" + 8083);
        clusterConfigDto.setClusterMap(map);
        clusterConfigDto.setFirstHand(true);
        Mockito.doNothing().when(listenReplicatorTask).switchListenReplicator(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt());
        switchService.switchListenReplicator(clusterConfigDto);
        Thread.sleep(1000);
        verify(listenReplicatorTask, times(1)).switchListenReplicator(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt());
        verify(broadCast, times(1)).broadcastWithRetry(Mockito.anyString(), Mockito.any(RequestMethod.class), Mockito.anyString(), Mockito.anyInt());
    }

    @Test
    public void testSwitchListenMessenger() throws Exception {
        ClusterConfigDto clusterConfigDto = new ClusterConfigDto();
        Map<String, String> map = new HashMap<>();
        map.put(CLUSTER_ID1, "ip");
        clusterConfigDto.setClusterMap(map);
        clusterConfigDto.setFirstHand(true);
        Mockito.doNothing().when(kafkaDelayMonitorServer).switchListenMessenger(Mockito.anyMap());
        switchService.switchListenMessenger(clusterConfigDto);
        Thread.sleep(1000);
        verify(kafkaDelayMonitorServer, times(1)).switchListenMessenger(Mockito.anyMap());
        verify(broadCast, times(1)).broadcastWithRetry(Mockito.anyString(), Mockito.any(RequestMethod.class), Mockito.anyString(), Mockito.anyInt());
    }


}
