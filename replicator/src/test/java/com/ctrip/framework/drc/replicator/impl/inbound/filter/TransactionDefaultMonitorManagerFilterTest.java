package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.monitor.kpi.InboundMonitorReport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/**
 * @Author limingdong
 * @create 2020/4/24
 */
public class TransactionDefaultMonitorManagerFilterTest extends AbstractFilterTest {

    @Mock
    private InboundMonitorReport inboundMonitorReport;

    @Mock
    private LogEventHeader logEventHeader;

    private TransactionMonitorFilter transactionMonitorFilter;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        transactionMonitorFilter = new TransactionMonitorFilter(inboundMonitorReport);

        when(gtidLogEvent.getGtid()).thenReturn(GTID);

        when(gtidLogEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(gtidLogEvent.getLogEventType()).thenReturn(LogEventType.gtid_log_event);

        when(xidLogEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(xidLogEvent.getLogEventType()).thenReturn(LogEventType.xid_log_event);

        when(logEventHeader.getEventSize()).thenReturn(EVENT_SIZE);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testGtidFalse() {
        Assert.assertFalse(transactionMonitorFilter.doFilter(logEventWithGroupFlag));
        verify(inboundMonitorReport, times(1)).addSize(EVENT_SIZE);
        verify(inboundMonitorReport, times(1)).addOneCount();
        verify(inboundMonitorReport, times(1)).addInboundGtid(1);
        verify(inboundMonitorReport, times(1)).addGtidStore(1, GTID);
    }

    @Test
    public void testGtidTrue() {
        logEventWithGroupFlag.setInExcludeGroup(true);
        Assert.assertTrue(transactionMonitorFilter.doFilter(logEventWithGroupFlag));
        verify(inboundMonitorReport, times(1)).addSize(EVENT_SIZE);
        verify(inboundMonitorReport, times(1)).addOneCount();
        verify(inboundMonitorReport, times(1)).addInboundGtid(1);
        verify(inboundMonitorReport, times(1)).addGtidFilter(1);
    }

    @Test
    public void testGtidTrueFakeXid() {
        logEventWithGroupFlag.setInExcludeGroup(true);
        logEventWithGroupFlag.setGtid(GTID);
        Assert.assertTrue(transactionMonitorFilter.doFilter(logEventWithGroupFlag));
        verify(inboundMonitorReport, times(1)).addSize(EVENT_SIZE);
        verify(inboundMonitorReport, times(1)).addOneCount();
        verify(inboundMonitorReport, times(1)).addInboundGtid(1);
        verify(inboundMonitorReport, times(1)).addGtidFilter(1);
        verify(inboundMonitorReport, times(1)).addXidFake(1);
    }

    @Test
    public void testXidFalse() {
        logEventWithGroupFlag.setLogEvent(xidLogEvent);
        Assert.assertFalse(transactionMonitorFilter.doFilter(logEventWithGroupFlag));
        verify(inboundMonitorReport, times(1)).addSize(EVENT_SIZE);
        verify(inboundMonitorReport, times(1)).addInboundXid(1);
    }

    @Test
    public void testXidTrue() {
        logEventWithGroupFlag.setLogEvent(xidLogEvent);
        logEventWithGroupFlag.setInExcludeGroup(true);
        Assert.assertTrue(transactionMonitorFilter.doFilter(logEventWithGroupFlag));
        verify(inboundMonitorReport, times(1)).addSize(EVENT_SIZE);
        verify(inboundMonitorReport, times(1)).addInboundXid(1);
    }

}