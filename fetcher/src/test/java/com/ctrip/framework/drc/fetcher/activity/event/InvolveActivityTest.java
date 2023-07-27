package com.ctrip.framework.drc.fetcher.activity.event;

import com.ctrip.framework.drc.core.driver.binlog.header.RowsEventPostHeader;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.fetcher.MockTest;
import com.ctrip.framework.drc.fetcher.event.FetcherRowsEvent;
import com.ctrip.framework.drc.fetcher.event.meta.MetaEvent;
import com.ctrip.framework.drc.fetcher.resource.context.BaseTransactionContext;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContext;
import com.ctrip.framework.drc.fetcher.system.TaskActivity;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/**
 * @Author limingdong
 * @create 2021/3/15
 */
public class InvolveActivityTest extends MockTest {

    private InvolveActivity involveActivity;

    @Mock
    private LinkContext linkContext;

    @Mock
    private LoadEventActivity loadEventActivity;

    @Mock
    private Columns columns;

    @Mock
    private TableKey tableKey;

    @Mock
    private TaskActivity taskActivity;

    private MetaEvent metaEvent = context -> {
        linkContext.namespace();
    };

    private FetcherRowsEvent rowsEvent = new FetcherRowsEvent() {
        @Override
        protected void doApply(BaseTransactionContext context) {

        }

        @Override
        public RowsEventPostHeader getRowsEventPostHeader() {
            RowsEventPostHeader rowsEventPostHeader = new RowsEventPostHeader() {
                @Override
                public long getTableId() {
                    return 0L;
                }

                @Override
                public int getFlags() {
                    return 0;
                }
            };
            return rowsEventPostHeader;
        }
    };

    @Before
    public void setUp() {
        super.initMocks();

        involveActivity = new InvolveActivity();
        involveActivity.linkContext = linkContext;
        involveActivity.loadEventActivity = loadEventActivity;

        involveActivity.link(taskActivity);
    }

    @Test
    public void testMetaEvent() throws InterruptedException {
        involveActivity.doTask(metaEvent);
        verify(linkContext, times(1)).namespace();
    }

    @Test
    public void testLoad() throws InterruptedException {
        when(linkContext.fetchTableKey()).thenReturn(tableKey);
        when(linkContext.fetchColumns(tableKey)).thenReturn(columns);
        when(linkContext.fetchColumns()).thenReturn(columns);
        when(linkContext.fetchTableKeyInMap(0L)).thenReturn(tableKey);
        involveActivity.doTask(rowsEvent);
        verify(loadEventActivity, times(1)).trySubmit(rowsEvent);
    }

}