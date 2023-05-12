package com.ctrip.framework.drc.core.server.common;

import com.ctrip.framework.drc.core.server.zookeeper.DrcZkConfig;
import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.lifecycle.Initializable;
import com.ctrip.xpipe.api.lifecycle.LifecycleState;
import com.ctrip.xpipe.cluster.ElectContext;
import com.ctrip.xpipe.lifecycle.DefaultLifecycleState;
import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Created by jixinwang on 2023/5/6
 */
public class AbstractResourceManagerTest {

    private String key = "test_cluster";

    @InjectMocks
    private TestAbstractResourceManager testAbstractResourceManager = new TestAbstractResourceManager();

    @Mock
    private LeaderElectorManager leaderElectorManager;

    @Mock
    private LeaderElector mockLeaderElector;

    @Mock
    private DrcZkConfig drcZkConfig;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        testAbstractResourceManager.setRestartDelayTime(0);
    }

    @Test
    public void getLeaderElector() throws Exception {
        Mockito.when(leaderElectorManager.createLeaderElector(Mockito.any(ElectContext.class))).thenReturn(mockLeaderElector);
        Mockito.doThrow(new KeeperException.ConnectionLossException()).when(mockLeaderElector).start();
        LifecycleState lifecycleState = new DefaultLifecycleState(mockLeaderElector, null);
        lifecycleState.setPhaseName(Initializable.PHASE_NAME_END);
        Mockito.when(mockLeaderElector.getLifecycleState()).thenReturn(lifecycleState);
        LeaderElector leaderElector = testAbstractResourceManager.getLeaderElector(key);
        Thread.sleep(100);
        Mockito.verify(mockLeaderElector, Mockito.atLeast(2)).start();
    }

    class TestAbstractResourceManager extends AbstractResourceManager {

    }
}