package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.fetcher.MockTest;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemory;
import com.ctrip.framework.drc.fetcher.resource.context.BaseTransactionContext;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContext;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContextResource;
import com.ctrip.framework.drc.fetcher.resource.transformer.TransformerContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * @Author limingdong
 * @create 2021/3/24
 */
public class MonitoredTableMapEventTest extends MockTest {

    @Mock
    private BaseTransactionContext transactionContext;

    @Before
    public void setUp() {
        super.initMocks();
    }

    @Test
    public void asApplyEvent() {
        MonitoredTableMapEvent testEvent = spy(new MonitoredTableMapEvent());
        LogEventHeader logEventHeader = spy(new LogEventHeader());
        when(testEvent.getLogEventHeader()).thenReturn(logEventHeader);
        when(testEvent.getSchemaName()).thenReturn("prod");
        when(testEvent.getTableName()).thenReturn("hello");
        testEvent.setDirectMemory(mock(DirectMemory.class));
        testEvent.involve(mock(LinkContextResource.class));
        testEvent.transformer(mock(TransformerContext.class));
        testEvent.apply(transactionContext);
        verify(transactionContext, times(1)).updateTableKeyMap(0L, TableKey.from("prod", "hello"));
    }

    @Test
    public void asMetaEvent() {
        MockTableMapEvent testEvent = new MockTableMapEvent(
                "prod", "hello");
        LinkContext context = spy(new LinkContextResource());
        context.resetTableKeyMap();
        doReturn(1).when(context).fetchDataIndex();
        doReturn("unset").when(context).fetchGtid();
        LogEventHeader logEventHeader = spy(new LogEventHeader());
        testEvent.setLogEventHeader(logEventHeader);
        testEvent.involve(context);
        assertEquals(TableKey.from("prod", "hello"), context.fetchTableKeyInMap(0L));
    }
}
