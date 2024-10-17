package com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.*;

/**
 * @author yongnian
 * @create 2024/10/15 21:03
 */
public class TransactionOffsetFilterTest {

    @Test
    public void testHasRows() {
        TransactionOffsetFilter transactionOffsetFilter = new TransactionOffsetFilter();

        DdlIndexFilter ddlIndexFilter = new DdlIndexFilter();
        ddlIndexFilter.setSuccessor(transactionOffsetFilter);


        TransactionEvent transactionEvent = new TransactionEvent();
        transactionEvent.addFilterLogEvent();

        GtidLogEvent gtidLogEvent = getGtidLogEvent(50);
        transactionEvent.addLogEvent(gtidLogEvent);
        transactionEvent.addLogEvent(getTableMapLogEvent(100L));
        transactionEvent.addLogEvent(getTableMapLogEvent(100L));
        transactionEvent.addLogEvent(getWriteRowsLogEvent(100L));
        transactionEvent.addLogEvent(getXidLogEvent(50));
        ddlIndexFilter.doFilter(transactionEvent);
        FilterLogEvent filterLogEvent = (FilterLogEvent) transactionEvent.getEvents().get(0);
        Assert.assertFalse(filterLogEvent.isNoRowsEvent());

        int allSum = 50 + 100 + 100 + 100 + 50;
        Assert.assertEquals(allSum, filterLogEvent.getNextTransactionOffset());
        verify(gtidLogEvent, times(1)).setNextTransactionOffsetAndUpdateEventSize(eq(allSum - 50L));
    }

    @Test
    public void testNowRows() {
        TransactionOffsetFilter transactionOffsetFilter = new TransactionOffsetFilter();

        DdlIndexFilter ddlIndexFilter = new DdlIndexFilter();
        ddlIndexFilter.setSuccessor(transactionOffsetFilter);


        TransactionEvent transactionEvent = new TransactionEvent();
        transactionEvent.addFilterLogEvent();

        GtidLogEvent gtidLogEvent = getGtidLogEvent(50);
        transactionEvent.addLogEvent(gtidLogEvent);
        transactionEvent.addLogEvent(getTableMapLogEvent(100L));
        transactionEvent.addLogEvent(getTableMapLogEvent(100L));
        transactionEvent.addLogEvent(getXidLogEvent(50));
        ddlIndexFilter.doFilter(transactionEvent);
        FilterLogEvent filterLogEvent = (FilterLogEvent) transactionEvent.getEvents().get(0);
        Assert.assertTrue(filterLogEvent.isNoRowsEvent());

        Assert.assertEquals(50 + 100 + 100 + 50, filterLogEvent.getNextTransactionOffset());
        verify(gtidLogEvent, times(1)).setNextTransactionOffsetAndUpdateEventSize(eq(250L));
    }

    private static TableMapLogEvent getTableMapLogEvent(long size) {
        TableMapLogEvent tableMapLogEvent = mock(TableMapLogEvent.class);
        when(tableMapLogEvent.getTableName()).thenReturn("mockTable");
        when(tableMapLogEvent.getSchemaName()).thenReturn("mockDB");
        LogEventHeader logEventHeader = getLogEventHeader(LogEventType.table_map_log_event.getType(), size);
        when(tableMapLogEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(tableMapLogEvent.getLogEventType()).thenReturn(LogEventType.table_map_log_event);
        return tableMapLogEvent;
    }

    private static LogEventHeader getLogEventHeader(int eventType, long size) {
        LogEventHeader logEventHeader = mock(LogEventHeader.class);
        when(logEventHeader.getEventSize()).thenReturn(size);
        when(logEventHeader.getEventType()).thenReturn(eventType);
        return logEventHeader;
    }

    private static WriteRowsEvent getWriteRowsLogEvent(long size) {
        WriteRowsEvent mock = mock(WriteRowsEvent.class);
        LogEventHeader logEventHeader = getLogEventHeader(LogEventType.write_rows_event_v2.getType(), size);
        when(mock.getLogEventHeader()).thenReturn(logEventHeader);
        when(mock.getLogEventType()).thenReturn(LogEventType.write_rows_event_v2);

        return mock;
    }

    private static XidLogEvent getXidLogEvent(int size) {
        XidLogEvent mock = mock(XidLogEvent.class);
        LogEventHeader logEventHeader = getLogEventHeader(LogEventType.xid_log_event.getType(), size);
        when(mock.getLogEventHeader()).thenReturn(logEventHeader);
        when(mock.getLogEventType()).thenReturn(LogEventType.xid_log_event);

        return mock;
    }

    private static GtidLogEvent getGtidLogEvent(int size) {
        GtidLogEvent mock = mock(GtidLogEvent.class);
        LogEventHeader logEventHeader = getLogEventHeader(LogEventType.gtid_log_event.getType(), (long) size);
        when(mock.getLogEventHeader()).thenReturn(logEventHeader);
        when(mock.getLogEventType()).thenReturn(LogEventType.gtid_log_event);

        return mock;
    }
}
