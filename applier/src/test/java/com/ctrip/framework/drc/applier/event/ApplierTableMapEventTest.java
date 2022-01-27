package com.ctrip.framework.drc.applier.event;

import com.ctrip.framework.drc.applier.resource.context.DecryptedTransactionContextResource;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemory;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContext;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContextResource;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * @Author Slight
 * Oct 16, 2019
 */
public class ApplierTableMapEventTest {

    @Test
    public void asApplyEvent() {
        ApplierTableMapEvent testEvent = spy(new ApplierTableMapEvent());
        LogEventHeader logEventHeader = spy(new LogEventHeader());
        when(testEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(testEvent.getSchemaName()).thenReturn("prod");
        when(testEvent.getTableName()).thenReturn("hello");
        DecryptedTransactionContextResource context = mock(DecryptedTransactionContextResource.class);
        testEvent.setDirectMemory(mock(DirectMemory.class));
        testEvent.involve(mock(LinkContextResource.class));
        testEvent.apply(context);
        verify(context, times(1)).updateTableKeyMap(0L, TableKey.from("prod", "hello"));
    }

    @Test
    public void asMetaEvent() {
        MockTableMapEvent testEvent = new MockTableMapEvent(
                "prod", "hello");
        LinkContext context = spy(new LinkContextResource());
        doReturn(1).when(context).fetchDataIndex();
        doReturn("unset").when(context).fetchGtid();
        LogEventHeader logEventHeader = spy(new LogEventHeader());
        testEvent.setLogEventHeader(logEventHeader);
        testEvent.involve(context);
        assertEquals(TableKey.from("prod", "hello"), context.fetchTableKey());
    }
}
