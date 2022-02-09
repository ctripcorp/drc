package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.binlog.header.RowsEventPostHeader;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemory;
import com.ctrip.framework.drc.fetcher.resource.context.BaseTransactionContext;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * @Author limingdong
 * @create 2021/3/24
 */
public class MonitoredUpdateRowsEventTest implements ColumnsRelatedTest {

    @Mock
    private BaseTransactionContext context;

    @Mock
    private LogEventHeader logEventHeader;

    @Mock
    RowsEventPostHeader rowsEventPostHeader;

    private DecryptedUpdateRowsEvent event;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(context.fetchTableKey()).thenReturn(TableKey.from("prod", "hello"));
    }

    @Test
    public void simpleUse() {
        event = spy(new DecryptedUpdateRowsEvent(columns1()));
        when(event.getAfterPresentRowsValues()).thenReturn(Lists.<List<Object>>newArrayList(Lists.newArrayList(1, "Dan", "Female", "2019-12-09 16:00:00.000")));
        when(event.getLogEventHeader()).thenReturn(logEventHeader);
        when(event.getAfterRowsKeysPresent()).thenReturn(Lists.newArrayList(true, true, true, true));
        when(event.getBeforePresentRowsValues()).thenReturn(Lists.<List<Object>>newArrayList(Lists.newArrayList(1, "Phy", "Male", "2019-12-09 15:00:00.000")));
        when(event.getBeforeRowsKeysPresent()).thenReturn(Lists.newArrayList(true, true, true, true));
        when(event.getRowsEventPostHeader()).thenReturn(rowsEventPostHeader);
        doNothing().when(event).load(columns1());
        event.setDirectMemory(mock(DirectMemory.class));
        event.apply(context);
        verify(context, times(1)).update(
                eq(Lists.<List<Object>>newArrayList(Lists.newArrayList(1, "Phy", "Male", "2019-12-09 15:00:00.000"))),
                eq(Bitmap.from(true, true, true, true)),
                eq(Lists.<List<Object>>newArrayList(Lists.newArrayList(1, "Dan", "Female", "2019-12-09 16:00:00.000"))),
                eq(Bitmap.from(true, true, true, true)),
                eq(columns1())
        );
    }

    @Test
    public void whenLoadThrowsException() {
        event = spy(new DecryptedUpdateRowsEvent(columns1()));
        doThrow(new RuntimeException("something does wrong")).when(event).load(columns1());
        event.apply(context);
        verify(context, never()).update(any(), any(), any(), any(), any());
    }

    @Test
    public void hostnameAndTime() {
        event = spy(new DecryptedUpdateRowsEvent(columns2()));
        when(event.getAfterPresentRowsValues()).thenReturn(Lists.<List<Object>>newArrayList(Lists.newArrayList("VMS121674", "2019-12-06 20:22:36")));
        when(event.getAfterRowsKeysPresent()).thenReturn(Lists.newArrayList(true, true));
        when(event.getBeforePresentRowsValues()).thenReturn(Lists.<List<Object>>newArrayList(Lists.newArrayList("VMS121674", "2019-12-06 20:22:25")));
        when(event.getBeforeRowsKeysPresent()).thenReturn(Lists.newArrayList(true, true));
        doNothing().when(event).load(any());
        event.apply(context);
    }
}
