package com.ctrip.framework.drc.console.monitor.delay;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.core.mq.DelayMessageConsumer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Set;


public class MqDelayMonitorServerTest {
    
    @InjectMocks private MqDelayMonitorServer mqDelayMonitorServer;

    @Mock private MonitorTableSourceProvider monitorProvider;
    @Mock private DefaultConsoleConfig consoleConfig;
    @Mock private DelayMessageConsumer consumer;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        Mockito.when(monitorProvider.getMqDelayMonitorSwitch()).thenReturn("on");
        Mockito.when(monitorProvider.getMqDelaySubject()).thenReturn("bbz.drc.delaymonitor");
        Mockito.when(monitorProvider.getMqDelayConsumerGroup()).thenReturn("100023928");
        Mockito.when(consoleConfig.getDcsInLocalRegion()).thenReturn(Set.of("shaxy"));
        Mockito.when(consumer.resumeListen()).thenReturn(true);
        Mockito.when(consumer.stopListen()).thenReturn(true);
    }

    @Test
    public void testTestAfterPropertiesSet() throws Exception {
        mqDelayMonitorServer.afterPropertiesSet();
    }

    @Test
    public void testTestIsleader() {
        mqDelayMonitorServer.isleader();
    }

    @Test
    public void testTestNotLeader() {
        mqDelayMonitorServer.notLeader();
    }
}