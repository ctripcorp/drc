package com.ctrip.framework.drc.manager.ha.localdc;

import com.ctrip.framework.drc.manager.ha.cluster.impl.InstanceStateController;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.concurrent.Executors;

/**
 * Created by jixinwang on 2022/11/16
 */
public class LocalDcNotifierTest extends AbstractDbClusterTest {

    @InjectMocks
    private LocalDcNotifier localDcNotifier = new LocalDcNotifier();

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Mock
    private InstanceStateController instanceStateController;


    @Before
    public void setUp() throws Exception {
        super.setUp();
        localDcNotifier.setExecutors(Executors.newFixedThreadPool(1));

    }

    @Test
    public void replicatorActiveElected() throws InterruptedException {
        localDcNotifier.replicatorActiveElected(CLUSTER_ID, null);
        verify(instanceStateController, times(0)).addMessenger(anyString(), anyObject());

        when(currentMetaManager.getActiveMessenger(CLUSTER_ID)).thenReturn(newMessenger);
        localDcNotifier.replicatorActiveElected(CLUSTER_ID, null);
        Thread.sleep(100);
        verify(instanceStateController, times(1)).addMessenger(anyString(), anyObject());
    }
}
