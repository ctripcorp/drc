package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.fetcher.MockTest;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemory;
import com.ctrip.framework.drc.fetcher.resource.context.BaseTransactionContext;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.List;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;

/**
 * @Author limingdong
 * @create 2021/3/24
 */
public class MonitoredDeleteRowsEventTest extends MockTest implements ColumnsRelatedTest {

    @Mock
    private BaseTransactionContext context;

    @Mock
    private LogEventHeader logEventHeader;

    private MonitoredDeleteRowsEvent event;

    @Before
    public void setUp() {
        super.initMocks();
        when(context.fetchTableKey()).thenReturn(TableKey.from("prod", "hello"));
        event = spy(new DecryptedDeleteRowsEvent(columns1()));
    }

    @Test
    public void testSimpleDelete() {
        when(event.getLogEventHeader()).thenReturn(logEventHeader);
        when(event.getBeforePresentRowsValues()).thenReturn(Lists.<List<Object>>newArrayList(Lists.newArrayList(1, "Mag", "Female", "2019-12-09 16:00:00.000")));
        when(event.getBeforeRowsKeysPresent()).thenReturn(Lists.newArrayList(true, true, true, true));
        doNothing().when(event).load(Mockito.any());
        event.setDirectMemory(mock(DirectMemory.class));
        event.apply(context);
        verify(context, times(1)).delete(
                eq(Lists.<List<Object>>newArrayList(Lists.newArrayList(1, "Mag", "Female", "2019-12-09 16:00:00.000"))), eq(Bitmap.from(true, true, true, true)),
                eq(columns1())
        );
    }

}