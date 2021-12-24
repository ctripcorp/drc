package com.ctrip.framework.drc.applier.resource.condition;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;

import static org.junit.Assert.*;

/**
 * @Author Slight
 * Sep 24, 2019
 */
public class LWMResourceTest {

    LWMResource lwm;

    @Before
    public void setUp() throws Exception {
        lwm = new LWMResource();
        lwm.initialize();
    }

    @After
    public void tearDown() throws Exception {
        lwm.dispose();
        lwm = null;
    }

    @Test
    public void simpleUse() throws Exception {
        //That no records in chart means that applier is doing no transactions.

        //first event, so pass, lwm = 98
        lwm.acquire(99);
        assertTrue(!lwm.tryPass(99));
        assertFalse(!lwm.tryPass(98));
        assertEquals(Lists.newArrayList((long)98), ((LWMResource.InnerBucket)lwm.bucket).list);

        //lwm = 98, so pass
        lwm.acquire(100);
        assertEquals(Lists.newArrayList((long)98), ((LWMResource.InnerBucket)lwm.bucket).list);
        //lwm = 98
        lwm.commit(99);
        //lwm = 99
        assertEquals(Lists.newArrayList((long)99), ((LWMResource.InnerBucket)lwm.bucket).list);
        assertEquals(99, lwm.bucket.lwm());
        try {
            //lwm = 99, 24 < 99, so assert fail
            lwm.commit(24);
        } catch (AssertionError e) {
            assertEquals("sequence number(24) must be larger than lwm(99)", e.getMessage());
        }

        //lwm = 99, so pass, last sequence number = 100, add 101 to bucket
        lwm.acquire(102);
        //bucket = [99, 101], lwm = 99
        assertTrue(!lwm.tryPass(100));
        assertFalse(!lwm.tryPass(99));

        lwm.commit(100);
        assertFalse(!lwm.tryPass(100));
        //bucket = [101], lwm = 101
        assertTrue(!lwm.tryPass(102));
        lwm.commit(102);
        //bucket = [102], lwm = 102

        //last sequence number = 102, add 103 to bucket
        //so bucket = [103], lwm = 103
        lwm.acquire(104);
        assertFalse(!lwm.tryPass(103));
    }

    @Test
    public void bug2019_12_27_16_33() throws InterruptedException {
        lwm.acquire(1509839);
        lwm.commit(1509839);
        lwm.acquire(1621974);
        lwm.commit(1621974);
        lwm.acquire(1079862);
        assertEquals(1079861, lwm.bucket.lwm());
        lwm.commit(1079862);
        assertEquals(1079862, lwm.bucket.lwm());
    }

    @Test
    public void reset() throws InterruptedException {
        lwm.acquire(101);
        new Thread(()-> {
            try {
                lwm.commit(101);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
        lwm.acquire(1);
        assertFalse(lwm.tryPass(101));
    }

    @Test
    public void onCommit() throws InterruptedException {
        lwm.tryPass(100);
        Thread t = new Thread(()-> {
            try {
                lwm.commit(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        t.start();
        t.join();
        assertTrue(lwm.tryPass(100));
    }

    @Test
    public void bugPairRC() throws InterruptedException {
        //first event
        lwm.acquire(959734);
        lwm.acquire(959747);
        lwm.commit(959747);
        assertTrue(!lwm.tryPass(959747));
    }

    @Test
    public void reconnect0() throws InterruptedException {
        lwm.acquire(100);
        lwm.commit(100);
        lwm.acquire(101);
        new Thread(()-> {
            try {
                lwm.commit(101);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
        lwm.acquire(101);
        assertTrue(lwm.tryPass(100));
    }

    @Test
    public void reconnect1() throws InterruptedException {
        lwm.acquire(101);
        new Thread(()-> {
            try {
                lwm.commit(101);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
        lwm.acquire(101);
        assertTrue(lwm.tryPass(100));
    }

    @Test
    public void onPass() throws InterruptedException {
        ArrayBlockingQueue<Integer> result = new ArrayBlockingQueue<>(1);
        lwm.acquire(101);
        lwm.onCommit(101, ()->result.offer(1));
        assertNull(result.poll());
        new Thread(()-> {
            try {
                lwm.commit(101);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
        assertEquals((Integer) 1, result.take());
    }

    @Test
    public void onPassWithGap() throws InterruptedException {
        ArrayBlockingQueue<Integer> result = new ArrayBlockingQueue<>(1);
        lwm.acquire(101);
        lwm.acquire(102);
        lwm.acquire(103);
        lwm.acquire(104);
        lwm.acquire(105);
        lwm.onCommit(104, ()->result.offer(1));
        assertNull(result.poll());
        Thread t0 = new Thread(()->{
            try {
                lwm.commit(101);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                lwm.commit(102);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                lwm.commit(104);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        t0.start();
        t0.join();
        assertNull(result.poll());
        Thread t1 = new Thread(()->{
            try {
                lwm.commit(103);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        t1.start();
        t1.join();
        assertEquals((Integer) 1, result.poll());
    }

    @Test
    public void onPassAfterCommit() throws InterruptedException {
        ArrayBlockingQueue<Integer> result = new ArrayBlockingQueue<>(1);
        lwm.acquire(101);
        lwm.commit(101);
        lwm.onCommit(101, ()->result.offer(1));
        assertEquals((Integer) 1, result.poll());
    }

    @Test
    public void bug20200701() throws InterruptedException {
        ArrayBlockingQueue<Integer> result = new ArrayBlockingQueue<>(1);
        lwm.acquire(10621);
        lwm.acquire(10623);
        lwm.onCommit(10622, ()->{
            result.offer(1);
        });
        lwm.commit(10621);
        result.take();
    }

    @Test
    public void concurrentUseOfOffsetNotifier() throws InterruptedException {
        LWMResource.InnerNotifier notifier = new LWMResource.InnerNotifier();
        Thread thread = new Thread(()-> {
            notifier.offsetIncreased(2);
        });
        thread.start();
        notifier.offsetIncreased(1);
        thread.join();
        assert notifier.await(2, 0);
    }

}
