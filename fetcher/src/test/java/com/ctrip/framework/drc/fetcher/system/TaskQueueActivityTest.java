package com.ctrip.framework.drc.fetcher.system;

import com.ctrip.framework.drc.fetcher.resource.thread.ExecutorResource;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * @Author limingdong
 * @create 2021/3/15
 */
public class TaskQueueActivityTest {

    public static ExecutorResource executor;
    public static ArrayBlockingQueue<Integer> queue;

    @Before
    public void setUp() throws Exception {
        executor = new ExecutorResource();
        executor.initialize();
        queue = new ArrayBlockingQueue<>(1);
    }

    class A extends TaskQueueActivity<Integer, Integer> {

        public A() {
            this.executor = TaskQueueActivityTest.executor;
        }

        @Override
        public Integer doTask(Integer task) throws InterruptedException {
            return hand(task + 1);
        }
    }

    class B extends TaskQueueActivity<Integer, Boolean> {

        public B() {
            this.executor = TaskQueueActivityTest.executor;
        }

        @Override
        public Integer doTask(Integer task) throws InterruptedException {
            System.out.println(task);
            queue.put(task);
            return finish(task);
        }
    }

    @Test
    public void testSimple() throws Exception {
        A source = new A();
        A a0 = new A();
        A a1 = new A();
        A a2 = new A();
        source.initialize();
        source.start();
        a0.initialize();
        a0.start();
        a1.initialize();
        a1.start();
        a2.initialize();
        a2.start();
        B term = new B();
        term.initialize();
        term.start();
        source.link(a0).link(a1).link(a2).link(term);
        source.trySubmit(0);
        assertEquals((Integer)4, queue.take());
    }

    class ErrorActivity extends TaskQueueActivity<AtomicInteger, Integer> {

        public ErrorActivity() {
            this.executor = TaskQueueActivityTest.executor;
        }

        @Override
        public AtomicInteger doTask(AtomicInteger number) throws InterruptedException {
            if (number.get() < 2) {
                number.addAndGet(1);
                throw new RuntimeException();
            }
            queue.put(number.get());
            return number;
        }
    }

    @Test
    public void retryIn2SWhenThrowable() throws Exception {
        long start = System.currentTimeMillis();
        ErrorActivity activity = new ErrorActivity();
        activity.initialize();
        activity.start();
        activity.waitSubmit(new AtomicInteger(1));
        assertEquals((Integer)2, queue.take());
        long now = System.currentTimeMillis();
        System.out.println("time waited: " + (now - start) + "ms");
        assertTrue(now - start >= 2000);
    }

    class FailureActivity extends TaskQueueActivity<AtomicInteger, Integer> {

        private int retries = 0;

        public FailureActivity() {
            this.executor = TaskQueueActivityTest.executor;
        }

        @Override
        public AtomicInteger doTask(AtomicInteger number) throws InterruptedException {
            logger.info("retry {}", ++retries);
            return retry(number, TimeUnit.MILLISECONDS, 0, 10, 50, 10, (e)-> {
                queue.put(retries);
                return null;
            });
        }
    }

    @Test
    public void retryWithDeltaAndLimit() throws Exception {
        long start = System.currentTimeMillis();
        FailureActivity activity = new FailureActivity();
        activity.initialize();
        activity.start();
        activity.waitSubmit(new AtomicInteger(1));
        assertEquals((Integer) 11, queue.take());
        long now = System.currentTimeMillis();
        System.out.println("time waited: " + (now - start) + "ms");
        assertTrue(now - start >= 350);
    }

}