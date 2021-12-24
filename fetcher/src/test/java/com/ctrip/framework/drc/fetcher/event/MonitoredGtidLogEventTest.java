package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.fetcher.MockTest;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemory;
import com.ctrip.framework.drc.fetcher.resource.context.BaseTransactionContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;

/**
 * @Author limingdong
 * @create 2021/3/24
 */
public class MonitoredGtidLogEventTest extends MockTest {

    private static final String GTID = "a0780e56-d445-11e9-97b4-58a328e0e9f2:5";

    @Mock
    private BaseTransactionContext transactionContext;

    @Before
    public void setUp() {
        super.initMocks();
    }

    @Test
    public void asMetaEvent() {
        MockGtidEvent testEvent = new MockGtidEvent(GTID, 1, 2);
        MockLinkContext context = new MockLinkContext();
        context.updateGtidSet(new GtidSet(""));
        LogEventHeader logEventHeader = spy(new LogEventHeader());
        testEvent.setLogEventHeader(logEventHeader);
        testEvent.involve(context);
        assertEquals(GTID, context.updatedGtid);
    }

    @Test
    public void asApplyEvent() {
        long lastCommitted = 9;
        long sequenceNumber = 10;
        when(transactionContext.fetchSequenceNumber()).thenReturn((long) 10);
        MockGtidEvent e9_10 = new MockGtidEvent(GTID, lastCommitted, sequenceNumber);
        e9_10.setDirectMemory(mock(DirectMemory.class));
        e9_10.setLogEventHeader(mock(LogEventHeader.class));
        e9_10.apply(transactionContext);
        verify(transactionContext, times(1)).updateSequenceNumber(sequenceNumber);
    }

    @Test
    public void testGetGtid() {
        MonitoredGtidLogEvent applierGtidEvent = getGtidEvent();
        String gtid = applierGtidEvent.getGtid();
        Assert.assertEquals(GTID, gtid);
        gtid = applierGtidEvent.getGtid();
        Assert.assertEquals(GTID, gtid);
    }

    @Test
    public void testIdentifier() {
        MonitoredGtidLogEvent applierGtidEvent = getGtidEvent();
        String identifier = applierGtidEvent.identifier();
        String id = applierGtidEvent.getGtid() + "(" + applierGtidEvent.getLastCommitted() + ", " + applierGtidEvent.getSequenceNumber() + ")";
        Assert.assertEquals(identifier, id);
        identifier = applierGtidEvent.identifier();
        Assert.assertEquals(identifier, id);
    }

    private MonitoredGtidLogEvent getGtidEvent() {
        return new MonitoredGtidLogEvent(GTID);
    }

}