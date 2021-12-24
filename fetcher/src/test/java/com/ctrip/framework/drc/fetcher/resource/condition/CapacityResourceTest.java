package com.ctrip.framework.drc.fetcher.resource.condition;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @Author limingdong
 * @create 2021/3/9
 */
public class CapacityResourceTest {

    CapacityResource capacity;

    @Before
    public void setUp() throws Exception {
        capacity = new CapacityResource();
        capacity.initialize();
    }

    @After
    public void tearDown() throws Exception {
        capacity.dispose();
        capacity = null;
    }

    @Test
    public void capacity() throws InterruptedException {
        for (int i = 1; i < 10001; i++) {
            capacity.tryAcquire(0, TimeUnit.SECONDS);
        }
        long start = System.currentTimeMillis();
        Thread t1 = new Thread(()->{
            try {
                TimeUnit.SECONDS.sleep(1);
                capacity.release();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        t1.start();
        assertFalse(capacity.tryAcquire(0, TimeUnit.SECONDS));
        assertTrue(capacity.tryAcquire(2, TimeUnit.SECONDS));
        long cost = System.currentTimeMillis() - start;
        System.out.println("time cost: " + cost + "ms");
        assert cost >= 1000;
    }
}