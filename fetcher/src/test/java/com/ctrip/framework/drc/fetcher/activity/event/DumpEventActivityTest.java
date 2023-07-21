package com.ctrip.framework.drc.fetcher.activity.event;

import com.ctrip.framework.drc.core.driver.MySQLConnector;
import com.ctrip.framework.drc.core.driver.binlog.LogEventCallBack;
import com.ctrip.framework.drc.core.driver.binlog.converter.ByteBufConverter;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.ctrip.framework.drc.core.exception.dump.BinlogDumpGtidException;
import com.ctrip.framework.drc.core.exception.dump.EventConvertException;
import com.ctrip.framework.drc.fetcher.MockTest;
import com.ctrip.framework.drc.fetcher.activity.replicator.FetcherSlaveServer;
import com.ctrip.framework.drc.fetcher.event.FetcherEvent;
import com.ctrip.framework.drc.fetcher.event.MonitoredGtidLogEvent;
import com.ctrip.framework.drc.fetcher.resource.condition.Capacity;
import com.ctrip.framework.drc.fetcher.resource.condition.ListenableDirectMemory;
import com.ctrip.framework.drc.fetcher.resource.context.NetworkContextResource;
import com.ctrip.framework.drc.fetcher.system.AbstractSystem;
import com.ctrip.framework.drc.fetcher.system.SystemStatus;
import com.ctrip.framework.drc.fetcher.system.TaskActivity;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.LifecycleState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.concurrent.TimeUnit;

/**
 * @Author limingdong
 * @create 2021/3/15
 */
public class DumpEventActivityTest extends MockTest {

    @Mock
    private MySQLSlaveConfig mySQLSlaveConfig;

    @Mock
    private MySQLConnector mySQLConnector;

    @Mock
    private ByteBufConverter byteBufConverter;

    @Mock
    private AbstractSystem abstractSystem;

    @Mock
    private LifecycleState lifecycleState;

    private Endpoint endpoint;

    private int count = 0;

    private TestDumpEventActivity dumpEventActivity;

    @Mock
    private MonitoredGtidLogEvent gtidLogEvent;

    @Mock
    private BinlogDumpGtidException dumpGtidException;

    @Mock
    private EventConvertException eventConvertException;

    @Mock
    private LogEventHeader logEventHeader;

    @Mock
    private ListenableDirectMemory listenableDirectMemory;

    @Mock
    private LogEventCallBack logEventCallBack;

    @Mock
    private Capacity capacity;

    @Mock
    private TaskActivity taskActivity;

    @Before
    public void setUp() {
        super.initMocks();
        dumpEventActivity = new TestDumpEventActivity();
        dumpEventActivity.setSystem(abstractSystem);
        dumpEventActivity.replicatorIp = "1.1.1.1";
        dumpEventActivity.replicatorPort = 123;
        endpoint = new DefaultEndPoint(dumpEventActivity.replicatorIp, dumpEventActivity.replicatorPort);
        dumpEventActivity.listenableDirectMemory = listenableDirectMemory;
        dumpEventActivity.capacity = capacity;

        dumpEventActivity.link(taskActivity);
    }

    @Test
    public void testOnLogEvent() throws Exception {
        when(mySQLSlaveConfig.getEndpoint()).thenReturn(endpoint);
        when(mySQLConnector.getLifecycleState()).thenReturn(lifecycleState);

        dumpEventActivity.initialize();
        dumpEventActivity.start();
        dumpEventActivity.stop();
        dumpEventActivity.dispose();

        Assert.assertEquals(count, 1);

        //test exception
        dumpEventActivity.getLogEventHandler().onLogEvent(null, null, dumpGtidException);
        verify(abstractSystem, times(0)).stop();
        dumpEventActivity.getLogEventHandler().onLogEvent(null, null, eventConvertException);
        verify(abstractSystem, times(1)).setStatus(SystemStatus.STOPPED);

        //test capacity return false
        when(gtidLogEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(logEventHeader.getEventSize()).thenReturn(2l);
        dumpEventActivity.getLogEventHandler().onLogEvent(gtidLogEvent, logEventCallBack, null);

        //test capacity return true and exec doHandleLogEvent
        when(capacity.tryAcquire(100, TimeUnit.MILLISECONDS)).thenReturn(true);
        dumpEventActivity.getLogEventHandler().onLogEvent(gtidLogEvent, logEventCallBack, null);

        Assert.assertEquals(count, 2);

    }

    class TestDumpEventActivity extends DumpEventActivity<FetcherEvent> {

        @Override
        protected FetcherSlaveServer getFetcherSlaveServer() {
            return new FetcherSlaveServer(mySQLSlaveConfig, mySQLConnector, byteBufConverter);
        }

        @Override
        protected NetworkContextResource getNetworkContextResource() throws Exception {
            return new NetworkContextResource();
        }

        protected void doHandleLogEvent(FetcherEvent logEvent) {
            count++;
        }

        @Override
        protected void doDispose() {
            count++;
        }
    }

}
