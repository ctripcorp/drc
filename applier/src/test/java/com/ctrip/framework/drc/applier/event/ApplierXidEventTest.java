package com.ctrip.framework.drc.applier.event;

import com.ctrip.framework.drc.applier.resource.context.DecryptedTransactionContextResource;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemory;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.junit.Test;

import static org.mockito.Mockito.*;

/**
 * @Author Slight
 * Oct 16, 2019
 */
public class ApplierXidEventTest {

    LogEventHeader logEventHeader = spy(new LogEventHeader());

    @Test
    public void terminateTransaction1() {
        //when there is more than 0 rows event that should not be force applied in case of conflict.
        // isConflict = true, shouldOverwrite = false.
        ApplierXidEvent testEvent = new ApplierXidEvent();
        DecryptedTransactionContextResource context = spy(new DecryptedTransactionContextResource());
        testEvent.setLogEventHeader(logEventHeader);
        doNothing().when(context).rollback();
        doNothing().when(context).setGtid(any());
        doNothing().when(context).begin();
        doNothing().when(context).commit();
        doReturn("GTID").when(context).fetchGtid();
        doReturn(10L).when(context).fetchSequenceNumber();
        doReturn(1L).when(context).fetchDelayMS();
        doReturn(Lists.newArrayList(true, true, true)).when(context).getConflictMap();
        doReturn(Lists.newArrayList(false, true, false)).when(context).getOverwriteMap();
        doReturn(Queues.newPriorityQueue()).when(context).getLogs();
        testEvent.setDirectMemory(mock(DirectMemory.class));
        testEvent.apply(context);
        verify(context, times(1)).rollback();
        verify(context, times(1)).setGtid(eq("GTID"));
        verify(context, times(1)).begin();
        verify(context, times(1)).commit();
    }

    @Test
    public void terminateTransaction2() {
        //many events encounter conflicts and all of them should be force applied.
        // isConflict = true, shouldOverwrite = true.
        ApplierXidEvent testEvent = new ApplierXidEvent();
        DecryptedTransactionContextResource context = spy(new DecryptedTransactionContextResource());
        testEvent.setLogEventHeader(logEventHeader);
        doNothing().when(context).rollback();
        doNothing().when(context).setGtid(any());
        doNothing().when(context).begin();
        doNothing().when(context).commit();
        doReturn("GTID").when(context).fetchGtid();
        doReturn(10L).when(context).fetchSequenceNumber();
        doReturn(1L).when(context).fetchDelayMS();
        doReturn(Lists.newArrayList(true, true, true)).when(context).getConflictMap();
        doReturn(Lists.newArrayList(true, true, true)).when(context).getOverwriteMap();
        doReturn(Queues.newPriorityQueue()).when(context).getLogs();
        testEvent.setDirectMemory(mock(DirectMemory.class));
        testEvent.apply(context);
        verify(context, never()).rollback();
        verify(context, times(1)).commit();
    }

    @Test
    public void terminateTransaction3() {
        //no conflicts
        ApplierXidEvent testEvent = new ApplierXidEvent();
        DecryptedTransactionContextResource context = spy(new DecryptedTransactionContextResource());
        testEvent.setLogEventHeader(logEventHeader);
        doNothing().when(context).rollback();
        doNothing().when(context).setGtid(any());
        doNothing().when(context).begin();
        doNothing().when(context).commit();
        doReturn("GTID").when(context).fetchGtid();
        doReturn(10L).when(context).fetchSequenceNumber();
        doReturn(1L).when(context).fetchDelayMS();
        doReturn(Lists.newArrayList(false, false, false)).when(context).getConflictMap();
        doReturn(Lists.newArrayList()).when(context).getOverwriteMap();
        doReturn(Queues.newPriorityQueue()).when(context).getLogs();
        testEvent.setDirectMemory(mock(DirectMemory.class));
        testEvent.apply(context);
        verify(context, never()).rollback();
        verify(context, times(1)).commit();
    }

}