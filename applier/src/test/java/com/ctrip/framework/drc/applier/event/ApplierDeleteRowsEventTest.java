package com.ctrip.framework.drc.applier.event;

import com.ctrip.framework.drc.applier.resource.context.TransactionContext;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemory;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.mockito.Mockito.*;

/**
 * @Author Slight
 * Oct 24, 2019
 */
public class ApplierDeleteRowsEventTest implements ApplierColumnsRelatedTest {

    @Mock
    TransactionContext context;
    @Mock
    LogEventHeader logEventHeader;
    ApplierDeleteRowsEvent event;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(context.fetchTableKey()).thenReturn(TableKey.from("prod", "hello"));
        event = spy(new DecryptedDeleteRowsEvent(columns1()));
    }

    @Test
    public void simpleUse() {
        when(event.getLogEventHeader()).thenReturn(logEventHeader);
        when(event.getBeforePresentRowsValues()).thenReturn(Lists.<List<Object>>newArrayList(Lists.newArrayList(1, "Mag", "Female", "2019-12-09 16:00:00.000")));
        when(event.getBeforeRowsKeysPresent()).thenReturn(Lists.newArrayList(true, true, true, true));
        doNothing().when(event).load(any());
        event.setDirectMemory(mock(DirectMemory.class));
        event.apply(context);
        verify(context, times(1)).delete(
                eq(Lists.<List<Object>>newArrayList(Lists.newArrayList(1, "Mag", "Female", "2019-12-09 16:00:00.000"))), eq(Bitmap.from(true, true, true, true)),
                eq(columns1())
        );
    }

}