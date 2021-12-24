package com.ctrip.framework.drc.applier.confirmed.java8;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Executor {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private Runnable runnable(int i) {
        return new Runnable() {
            @Override
            public void run() {
                while (true) {
                    logger.info("i is {}", i);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
    }

    @Test
    public void twoTaskQueued() throws InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 3, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(2), new ThreadPoolExecutor.AbortPolicy()
        );
        executor.execute(runnable(1));
        executor.execute(runnable(2));
        executor.execute(runnable(3));
        Thread.currentThread().join();
    }

    @Test
    public void oneTaskQueuedAndUseNonCoreThread() throws InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 3, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1), new ThreadPoolExecutor.AbortPolicy()
        );
        executor.execute(runnable(1));
        executor.execute(runnable(2));
        executor.execute(runnable(3));
        Thread.currentThread().join();
    }

    @Test
    public void fullPowerRunningWithOneTaskQueued() throws InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 3, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1), new ThreadPoolExecutor.AbortPolicy()
        );
        executor.execute(runnable(1));
        executor.execute(runnable(2));
        executor.execute(runnable(3));
        executor.execute(runnable(4));
        Thread.currentThread().join();
    }

    @Test (expected = RejectedExecutionException.class)
    public void oneTaskRejected() throws InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 3, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1), new ThreadPoolExecutor.AbortPolicy()
        );
        executor.execute(runnable(1));
        executor.execute(runnable(2));
        executor.execute(runnable(3));
        executor.execute(runnable(4));
        executor.execute(runnable(5));
        Thread.currentThread().join();
    }
}
