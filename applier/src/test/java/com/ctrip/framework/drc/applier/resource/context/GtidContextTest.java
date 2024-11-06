package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.fetcher.resource.context.AbstractContext;
import com.ctrip.framework.drc.fetcher.resource.context.GtidContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * @Author Slight
 * Oct 27, 2019
 */
public class GtidContextTest {

    class TestContext extends AbstractContext implements GtidContext {}

    TestContext context;

    @Before
    public void setUp() throws Exception {
        context = new TestContext();
        context.initialize();
    }

    @After
    public void tearDown() throws Exception {
        context.dispose();
        context = null;
    }

    @Test
    public void simpleUse() {
        context.updateGtid("9f51357e-1cb1-11e8-a7f9-fa163e00d7c8:1-108240921");
        assertEquals("9f51357e-1cb1-11e8-a7f9-fa163e00d7c8:1-108240921", context.fetchGtid());
    }

    @Test
    public void testConcurrentlyFetch() throws Exception {
        ExecutorService executorService = ThreadUtils.newCachedThreadPool("identity");

        for (int j = 0; j < 10; j++) {
            TestContext testContext = new TestContext();
            testContext.initialize();

            testContext.updateGtid("some_value");
            List<Future<?>> futures = new ArrayList<>();
            List<Exception> exceptions = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                int finalI = i;
                Future<?> submit = executorService.submit(() -> {
                    testContext.update("key" + finalI, "value");
                    // should not throw exception
                    try {
                        testContext.fetchGtid();
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                });
                futures.add(submit);
            }
            for (Future<?> future : futures) {
                future.get(100, TimeUnit.MILLISECONDS);
            }
            Assert.assertEquals(exceptions.toString(), 0, exceptions.size());
        }

    }
}