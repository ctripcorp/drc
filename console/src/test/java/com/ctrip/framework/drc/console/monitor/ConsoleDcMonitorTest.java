package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.xpipe.zk.ZkClient;
import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Created by dengquanliang
 * 2025/3/4 14:16
 */
public class ConsoleDcMonitorTest {

    @InjectMocks
    private ConsoleDcMonitor consoleDcMonitor;

    @Mock
    private DefaultConsoleConfig consoleConfig;

    @Mock
    private ZkClient zkClient;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testScheduledTask() throws Throwable {
        Mockito.when(consoleConfig.isCenterRegion()).thenReturn(true);
        Mockito.when(consoleConfig.getDcsInLocalRegion()).thenReturn(Sets.newHashSet("ntgxh"));
        Mockito.when(zkClient.get()).thenReturn(Mockito.mock(CuratorFramework.class));
        consoleDcMonitor.initialize();
        consoleDcMonitor.setRegionLeader(true);
        consoleDcMonitor.switchToLeader();
    }

}
