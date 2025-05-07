package com.ctrip.framework.drc.manager.service;

import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.manager.config.DataCenterService;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;

/**
 * @Author limingdong
 * @create 2020/7/3
 */
public class ConsoleServiceImplTest extends AbstractDbClusterTest {

    @InjectMocks
    private ConsoleServiceImpl consoleService = new ConsoleServiceImpl();

    @Mock
    private DataCenterService dataCenterService;

    @Mock
    private ClusterManagerConfig clusterManagerConfig;

    @Mock
    private MysqlConsoleNotifier mysqlConsoleNotifier;

    @Mock
    private ReplicatorConsoleNotifier replicatorConsoleNotifier;

    @Mock
    private MessengerConsoleNotifier messengerConsoleNotifier;


    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        try {
            super.tearDown();
        } catch (Exception e) {
        }
    }


    @Test
    public void testReplicatorActiveElected() throws Exception {
        doNothing().when(replicatorConsoleNotifier).notifyMasterChanged(Mockito.anyString(), Mockito.anyString());
        consoleService.replicatorActiveElected(CLUSTER_ID, newReplicator);
        verify(replicatorConsoleNotifier, times(1)).notifyMasterChanged(Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void testMysqlMasterChanged() throws Exception {

        doNothing().when(mysqlConsoleNotifier).notifyMasterChanged(Mockito.anyString(), Mockito.anyString());
        consoleService.mysqlMasterChanged(CLUSTER_ID, mysqlMaster);
        verify(mysqlConsoleNotifier, times(1)).notifyMasterChanged(Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void testMessengerActiveElected() throws Exception {
        Messenger messenger = new Messenger();
        messenger.setApplyMode(ApplyMode.mq.getType());
        messenger.setIp("ip");

        doNothing().when(messengerConsoleNotifier).notifyMasterChanged(Mockito.anyString(), Mockito.anyString());
        consoleService.messengerActiveElected(CLUSTER_ID, messenger);
        verify(messengerConsoleNotifier, never()).notifyMasterChanged(Mockito.anyString(), Mockito.anyString());

        messenger.setApplyMode(ApplyMode.kafka.getType());
        consoleService.messengerActiveElected(CLUSTER_ID, messenger);
        verify(messengerConsoleNotifier, times(1)).notifyMasterChanged(Mockito.anyString(), Mockito.anyString());
    }


}
