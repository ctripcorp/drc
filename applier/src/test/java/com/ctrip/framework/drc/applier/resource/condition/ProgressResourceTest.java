package com.ctrip.framework.drc.applier.resource.condition;

import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import org.junit.After;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @Author limingdong
 * @create 2020/12/15
 */
public class ProgressResourceTest {

    private ProgressResource progressResource = new ProgressResource();

    private ExecutorService executorService = ThreadUtils.newFixedThreadPool(10, "ProgressResourceTest");

    @After
    public void tearDown() {
        executorService.shutdown();
    }

    @Test
    public void tick() throws InterruptedException {

        Random random = new Random();
        int limit = random.nextInt(10000);
        CountDownLatch countDownLatch = new CountDownLatch(limit);
        for(int i = 0; i < limit; ++i) {
                executorService.submit(() -> {
                    try {
                        progressResource.tick();
                    } finally {
                        countDownLatch.countDown();
                    }
                });
        }

        boolean finished = countDownLatch.await(2, TimeUnit.SECONDS);

        if (finished) {
            assertEquals(limit, progressResource.get());
            progressResource.clear();
            assertEquals(0, progressResource.get());
        } else {
            assertTrue(limit > progressResource.get());
        }
    }
}