package com.ctrip.framework.drc.core.monitor.util;

import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jixinwang on 2022/3/2
 */
public class IsolateHashCacheTest {

    @Test
    public void testIsolateHashCache() throws InterruptedException {
        ExecutorService put = ThreadUtils.newSingleThreadExecutor("123");
        ExecutorService remove = ThreadUtils.newSingleThreadExecutor("456");
        final IsolateHashCache<Integer, String> map = new IsolateHashCache<Integer, String>(1000, 16, 4);

        final AtomicInteger counter = new AtomicInteger(0);
        final AtomicInteger removeCounter = new AtomicInteger(0);

        final String blah = "BlahBlah";

        Thread t1 = new Thread(() -> {

            for (int i = counter.incrementAndGet(); i < 20000; i = counter.incrementAndGet()) {
                put.submit(() -> {
                    map.put(counter.incrementAndGet(), blah);
                });
            }
        });

        Thread t2 = new Thread(() -> {
            for (int i = counter.incrementAndGet(); i < 20000; i = counter.incrementAndGet()) {
                remove.submit(() -> {
                    int j = counter.get();
                    Object value = map.getIfPresent(j);
                    if (value != null) {
                        map.invalidate(j);
                        removeCounter.incrementAndGet();
                    }
                });
            }
        });

        t1.start();
        t2.start();
        t1.join();
        t2.join();
        Assert.assertTrue(1004 - map.size() >= 0);
    }
}
