package com.ctrip.framework.drc.replicator.impl.monitor;

import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.binlog.impl.ParsedDdlLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.UpdateRowsEvent;
import com.ctrip.framework.drc.replicator.impl.oubound.observer.MonitorEventObserver;
import com.ctrip.framework.drc.replicator.store.AbstractTransactionTest;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_DELAY_MONITOR_TABLE_NAME;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_MONITOR_SCHEMA_NAME;

/**
 * @Author limingdong
 * @create 2020/6/16
 */
public class DefaultMonitorManagerTest extends AbstractTransactionTest {

    private DefaultMonitorManager delayMonitor = new DefaultMonitorManager("ut");

    @Mock
    private MonitorEventObserver monitorEventObserver1;

    @Mock
    private MonitorEventObserver monitorEventObserver2;

    @Mock
    private TableMapLogEvent tableMapLogEvent;

    @Mock
    private UpdateRowsEvent updateRowsEvent;

    @Mock
    private LogEventHeader logEventHeader;

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        delayMonitor.addObserver(monitorEventObserver1);
        delayMonitor.addObserver(monitorEventObserver2);
        when(tableMapLogEvent.getSchemaName()).thenReturn(DRC_MONITOR_SCHEMA_NAME);
        when(tableMapLogEvent.getTableName()).thenReturn(DRC_DELAY_MONITOR_TABLE_NAME);
        when(updateRowsEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(updateRowsEvent.getPayloadBuf()).thenReturn(getXidEventHeader());
        when(logEventHeader.getHeaderBuf()).thenReturn(getXidEventHeader());
    }

    @After
    public void tearDown() {
        delayMonitor.removeObserver(monitorEventObserver1);
        delayMonitor.removeObserver(monitorEventObserver2);
    }

    @Test
    public void filterMonitorUpdateEvent() {
        delayMonitor.onTableMapLogEvent(tableMapLogEvent);
        delayMonitor.onUpdateRowsEvent(updateRowsEvent, GTID);
        verify(monitorEventObserver1, times(1)).update(anyObject(), anyObject());
        verify(monitorEventObserver2, times(1)).update(anyObject(), anyObject());
    }

    @Test
    public void testOnDdlLogEvent() {
        String DB = RandomStringUtils.randomAlphabetic(10);
        String TABLE = RandomStringUtils.randomAlphabetic(10);
        String DDL = "truncate table " + TABLE;

        MonitorEventObserver eventObserver = (args, observable) -> {
            Assert.assertTrue(args instanceof ParsedDdlLogEvent);
            if (args instanceof ParsedDdlLogEvent) {
                ParsedDdlLogEvent ddlLogEvent = (ParsedDdlLogEvent) args;
                Assert.assertEquals(ddlLogEvent.getSchema(), DB);
                Assert.assertEquals(ddlLogEvent.getTable(), TABLE);
                Assert.assertEquals(ddlLogEvent.getDdl(), DDL);
                Assert.assertEquals(ddlLogEvent.getQueryType(), QueryType.TRUNCATE);
            }
        };
        delayMonitor.addObserver(eventObserver);
        delayMonitor.onDdlEvent(DB, TABLE, DDL, QueryType.TRUNCATE);
    }
}
