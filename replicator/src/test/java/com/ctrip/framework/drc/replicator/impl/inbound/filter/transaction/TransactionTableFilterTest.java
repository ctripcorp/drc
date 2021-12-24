package com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.replicator.MockTest;
import com.ctrip.framework.drc.replicator.impl.monitor.MonitorManager;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.List;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.*;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_DELAY_MONITOR_TABLE_NAME;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_TRANSACTION_TABLE_NAME;


/**
 * Created by jixinwang on 2021/10/10
 */
public class TransactionTableFilterTest extends MockTest {

    private TransactionTableFilter transactionTableFilter;

    private TransactionEvent transactionEvent = new TransactionEvent();

    @Mock
    private MonitorManager monitorManager;

    @Mock
    private GtidLogEvent gtidLogEvent;

    @Mock
    private QueryLogEvent queryLogEvent;

    @Mock
    private RowsQueryLogEvent rowsQueryLogEvent;

    @Mock
    private TableMapLogEvent transactionTableTableMapLogEvent;

    @Mock
    private UpdateRowsEvent updateRowsEvent1;

    @Mock
    private TableMapLogEvent delayMonitorTableMapLogEvent;

    @Mock
    private UpdateRowsEvent updateRowsEvent2;

    @Mock
    private XidLogEvent xidLogEvent;

    @Before
    public void setUp() {
        transactionTableFilter = new TransactionTableFilter(monitorManager);
        transactionTableFilter.setSuccessor(new TransactionOffsetFilter());

        List<LogEvent> logEvents = Lists.newArrayList(gtidLogEvent, queryLogEvent, transactionTableTableMapLogEvent,
                updateRowsEvent1, delayMonitorTableMapLogEvent, updateRowsEvent2, xidLogEvent);
        transactionEvent.setEvents(logEvents);

        when(gtidLogEvent.getLogEventType()).thenReturn(gtid_log_event);
        when(queryLogEvent.getLogEventType()).thenReturn(query_log_event);
        when(rowsQueryLogEvent.getLogEventType()).thenReturn(rows_query_log_event);
        when(transactionTableTableMapLogEvent.getLogEventType()).thenReturn(table_map_log_event);
        when(updateRowsEvent1.getLogEventType()).thenReturn(update_rows_event_v2);
        when(delayMonitorTableMapLogEvent.getLogEventType()).thenReturn(table_map_log_event);
        when(updateRowsEvent2.getLogEventType()).thenReturn(update_rows_event_v2);
        when(xidLogEvent.getLogEventType()).thenReturn(xid_log_event);

        when(transactionTableTableMapLogEvent.getTableName()).thenReturn(DRC_TRANSACTION_TABLE_NAME);
        when(delayMonitorTableMapLogEvent.getTableName()).thenReturn(DRC_DELAY_MONITOR_TABLE_NAME);
    }

    @Test
    public void testTransactionTableFilter() {
        boolean isTransactionTable = transactionTableFilter.doFilter(transactionEvent);
        Assert.assertTrue(isTransactionTable);
    }
}
