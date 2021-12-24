package com.ctrip.framework.drc.fetcher.activity.monitor;

import com.ctrip.framework.drc.fetcher.MockTest;
import com.ctrip.framework.drc.fetcher.resource.thread.ExecutorResource;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author limingdong
 * @create 2021/3/24
 */
public class ReportActivityTest extends MockTest {

    private TestReportActivity reportActivity;

    private AtomicInteger count = new AtomicInteger(0);

    private ExecutorResource executor = new ExecutorResource();

    private int loopCount = 100;  // N % 50 == 0

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        executor.initialize();
        executor.start();
        reportActivity = new TestReportActivity();
        reportActivity.executor = executor;
        reportActivity.restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate();
        reportActivity.initialize();
        reportActivity.start();
    }

    @After
    public void tearDown() throws Exception {
        executor.stop();
        executor.dispose();
        reportActivity.stop();
        reportActivity.dispose();
    }

    @Test
    public void testDoTask() throws InterruptedException {

        for(int i = 0; i < loopCount; ++i) {
            reportActivity.report(count);
        }
        Thread.sleep(20);
        Assert.assertEquals(count.get(), loopCount);
    }

    class TestReportActivity extends ReportActivity<AtomicInteger, AtomicInteger> {

        @Override
        public boolean report(AtomicInteger task) {
            trySubmit(task);
            return false;
        }

        @Override
        public void doReport(List<AtomicInteger> taskList) {
            taskList.forEach(t -> t.getAndIncrement());
        }
    }

}