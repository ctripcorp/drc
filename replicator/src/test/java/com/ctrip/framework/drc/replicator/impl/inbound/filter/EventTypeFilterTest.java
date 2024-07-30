package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @Author limingdong
 * @create 2020/3/12
 */
public class EventTypeFilterTest extends AbstractFilterTest {

    private EventTypeFilter eventTypeFilter;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        eventTypeFilter = new EventTypeFilter("test");
    }

    @Test
    public void doFilterTrue() {
        when(gtidLogEvent.getLogEventType()).thenReturn(LogEventType.gtid_log_event);
        boolean skip = eventTypeFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);
    }

    @Test
    public void doFilterFlase() {
        when(gtidLogEvent.getLogEventType()).thenReturn(LogEventType.previous_gtids_log_event);
        boolean skip = eventTypeFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);
    }

    @Test
    public void doFilterDdl() {
        when(gtidLogEvent.getLogEventType()).thenReturn(LogEventType.drc_ddl_log_event);
        boolean skip = eventTypeFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);
    }

    @Test
    public void doFilterTransaction() {
        when(gtidLogEvent.getLogEventType()).thenReturn(LogEventType.drc_gtid_log_event);
        boolean skip = eventTypeFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);
    }

    @Test
    public void doFilterDrcHeartBeatEvent() {
        when(gtidLogEvent.getLogEventType()).thenReturn(LogEventType.drc_heartbeat_log_event);
        boolean skip = eventTypeFilter.doFilter(logEventWithGroupFlag);
        verify(callBack, times(1)).onHeartHeat();
        Assert.assertTrue(skip);
    }

    @Test
    public void doFilterUnknownEvent() {
        LogEventHeader logEventHeader = new LogEventHeader();
        logEventHeader.setEventType(28);
        when(drcUnknownEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(drcUnknownEvent.getLogEventType()).thenReturn(LogEventType.unknown_log_event);
        boolean skip = eventTypeFilter.doFilter(new InboundLogEventContext(drcUnknownEvent, callBack, new TransactionFlags(), ""));
        Assert.assertTrue(skip);
    }
}