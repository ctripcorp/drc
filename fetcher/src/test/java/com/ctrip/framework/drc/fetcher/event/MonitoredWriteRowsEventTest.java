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

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;

/**
 * @Author limingdong
 * @create 2021/3/24
 */
public class MonitoredWriteRowsEventTest implements ColumnsRelatedTest {

    @Mock
    private BaseTransactionContext context;

    @Mock
    private LogEventHeader logEventHeader;

    @Mock
    RowsEventPostHeader rowsEventPostHeader;

    private DecryptedWriteRowsEvent event;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(context.fetchTableKey()).thenReturn(TableKey.from("prod", "hello"));
        event = spy(new DecryptedWriteRowsEvent(columns1()));
    }

    @Test
    public void simpleUse() {
        List<List<Object>> values = Lists.<List<Object>>newArrayList(Lists.newArrayList(1, "Mag", "1:00"));
        List<Boolean> bitmap = Lists.newArrayList(true, true, false, true);
        when(event.getBeforePresentRowsValues()).thenReturn(values);
        when(event.getLogEventHeader()).thenReturn(logEventHeader);
        when(event.getBeforeRowsKeysPresent()).thenReturn(bitmap);
        when(event.getRowsEventPostHeader()).thenReturn(rowsEventPostHeader);
        event.setDirectMemory(mock(DirectMemory.class));
        doNothing().when(event).load(any());
        event.apply(context);
        verify(context, times(1)).insert(
                eq(values), eq(Bitmap.from(bitmap)),
                eq(columns1())
        );
    }

    @Test
    public void whenLoadThrowsException() {
        doThrow(new RuntimeException("something does wrong")).when(event).load(columns1());
        event.apply(context);
        verify(context, never()).insert(any(), any(), any());
        //error map of transaction context should be handled properly.
    }

    @Test
    public void testUsageAfterLoadRelease() {
        MonitoredWriteRowsEvent event = DecryptedWriteRowsEvent.mockSimpleEvent();
        event.setDirectMemory(mock(DirectMemory.class));
        event.load(DecryptedWriteRowsEvent.mockSimpleColumns());
        event.release();
        assertEquals("[[9, varchar]]", event.getBeforePresentRowsValues().toString());
        assertEquals("[true, false, false, true]", event.getBeforeRowsKeysPresent().toString());
    }

    @Test
    public void testReleaseTwice() throws InterruptedException {
        MonitoredWriteRowsEvent event = DecryptedWriteRowsEvent.mockSimpleEvent();
        event.setDirectMemory(mock(DirectMemory.class));
        Thread t = new Thread(()-> {
            event.load(DecryptedWriteRowsEvent.mockSimpleColumns());
        });
        t.start();
        t.join();
        event.release();
        assertEquals("[[9, varchar]]", event.getBeforePresentRowsValues().toString());
        assertEquals("[true, false, false, true]", event.getBeforeRowsKeysPresent().toString());
        t = new Thread(()-> {
            event.release();
        });
        t.start();
        t.join();
    }

}
