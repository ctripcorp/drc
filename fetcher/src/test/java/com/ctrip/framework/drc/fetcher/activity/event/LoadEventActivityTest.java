package com.ctrip.framework.drc.fetcher.activity.event;

import com.ctrip.framework.drc.fetcher.MockTest;
import com.ctrip.framework.drc.fetcher.event.FetcherRowsEvent;
import com.ctrip.framework.drc.fetcher.resource.thread.Executor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/**
 * @Author limingdong
 * @create 2021/3/24
 */
public class LoadEventActivityTest extends MockTest {

    private LoadEventActivity loadEventActivity;

    @Mock
    private FetcherRowsEvent rowsEvent;

    private int count = 0;

    private Executor executor = runnable -> count++;

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        loadEventActivity = new LoadEventActivity();
        loadEventActivity.executor = executor;
        loadEventActivity.initialize();
        loadEventActivity.start();
    }

    @After
    public void tearDown() throws Exception {
        loadEventActivity.stop();
        loadEventActivity.dispose();
    }

    @Test
    public void testDoTask() {
        loadEventActivity.doTask(rowsEvent);
        verify(rowsEvent, times(1)).tryLoad();
        Assert.assertEquals(count, 8);  // thread size
    }

}