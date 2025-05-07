package com.ctrip.framework.drc.applier.event;

import com.ctrip.framework.drc.fetcher.resource.condition.LWMResource;
import com.ctrip.framework.drc.applier.resource.context.DecryptedTransactionContextResource;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.fetcher.event.ApplierGtidEvent;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemory;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * @Author Slight
 * Oct 16, 2019
 */
public class ApplierGtidEventTest {

    private static final String GTID = "a0780e56-d445-11e9-97b4-58a328e0e9f2:5";

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
    public void firstEvent() throws Exception {
        LWMResource lwm = new LWMResource();
        lwm.initialize();
        lwm.acquire(10);
        lwm.tryPass(9);
    }

    @Test
    public void asApplyEvent() {
        long lastCommitted = 9;
        long sequenceNumber = 10;
        DecryptedTransactionContextResource context = mock(DecryptedTransactionContextResource.class);
        when(context.fetchSequenceNumber()).thenReturn((long)10);
        when(context.everConflict()).thenReturn(false);
        when(context.everRollback()).thenReturn(true);
        MockGtidEvent e9_10 = new MockGtidEvent(GTID, lastCommitted, sequenceNumber);
        e9_10.setDirectMemory(mock(DirectMemory.class));
        e9_10.setLogEventHeader(mock(LogEventHeader.class));
        e9_10.apply(context);
        verify(context, times(1)).setGtid(GTID);
        verify(context, times(1)).updateSequenceNumber(sequenceNumber);
    }

    @Test
    public void testGetGtid() {
        ApplierGtidEvent applierGtidEvent = getApplierGtidEvent();
        String gtid = applierGtidEvent.getGtid();
        Assert.assertEquals(GTID, gtid);
        gtid = applierGtidEvent.getGtid();
        Assert.assertEquals(GTID, gtid);
    }

    @Test
    public void testIdentifier() {
        ApplierGtidEvent applierGtidEvent = getApplierGtidEvent();
        String identifier = applierGtidEvent.identifier();
        String id = applierGtidEvent.getGtid() + "(" + applierGtidEvent.getLastCommitted() + ", " + applierGtidEvent.getSequenceNumber() + ")";
        Assert.assertEquals(identifier, id);
        identifier = applierGtidEvent.identifier();
        Assert.assertEquals(identifier, id);
    }

    private ApplierGtidEvent getApplierGtidEvent() {
        return new ApplierGtidEvent(GTID);
    }

}
