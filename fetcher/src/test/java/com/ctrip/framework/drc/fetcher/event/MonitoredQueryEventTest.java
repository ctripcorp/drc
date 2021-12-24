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

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;

/**
 * @Author limingdong
 * @create 2021/3/24
 */
public class MonitoredQueryEventTest extends MockTest {

    private static final String GTID = "a0780e56-d445-11e9-97b4-58a328e0e9f2:15";

    private int initDataIndex = Math.abs(new Random().nextInt(100));

    @Mock
    private BaseTransactionContext transactionContext;

    @Before
    public void setUp() {
        super.initMocks();
    }

    @Test
    public void testInvolve() {
        MonitoredQueryEvent testEvent = new MonitoredQueryEvent();
        MockLinkContext context = new MockLinkContext();
        context.updateGtidSet(new GtidSet(""));
        context.updateDataIndex(initDataIndex);
        LogEventHeader logEventHeader = spy(new LogEventHeader());
        testEvent.setLogEventHeader(logEventHeader);
        testEvent.involve(context);
        assertEquals(initDataIndex + 1, context.fetchDataIndex());
    }

    @Test
    public void testIdentifier() {
        MonitoredGtidLogEvent applierGtidEvent = new MonitoredGtidLogEvent(GTID);
        String identifier = applierGtidEvent.identifier();
        String id = applierGtidEvent.getGtid() + "(" + applierGtidEvent.getLastCommitted() + ", " + applierGtidEvent.getSequenceNumber() + ")";
        Assert.assertEquals(identifier, id);
        identifier = applierGtidEvent.identifier();
        Assert.assertEquals(identifier, id);
    }

    @Test
    public void asApplyEvent() {
        long lastCommitted = 10;
        long sequenceNumber = 11;
        when(transactionContext.fetchSequenceNumber()).thenReturn((long) 11);
        MockGtidEvent e10_11 = new MockGtidEvent(GTID, lastCommitted, sequenceNumber);
        e10_11.setDirectMemory(mock(DirectMemory.class));
        e10_11.setLogEventHeader(mock(LogEventHeader.class));
        e10_11.apply(transactionContext);
        verify(transactionContext, times(1)).updateSequenceNumber(sequenceNumber);
        verify(transactionContext, times(1)).updateGtid(GTID);
    }
}