package com.ctrip.framework.drc.fetcher.resource.thread;

import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * @Author limingdong
 * @create 2021/3/9
 */
public class ExecutorResourceTest {

    @Test
    public void allTogetherWithoutException() throws Exception {
        ExecutorResource tpe = new ExecutorResource();
        tpe.doInitialize();
        assertNotEquals(null, tpe.internal);
        tpe.execute(() -> {
        });
        tpe.doDispose();
        assertEquals(null, tpe.internal);
    }

    @Test
    public void usingInternal() throws Exception {
        class MockExecutorService implements ExecutorService {
            public int shutdownCalled = 0;
            public int executeCalled = 0;

            @Override
            public void shutdown() {
                shutdownCalled++;
            }

            @Override
            public List<Runnable> shutdownNow() {
                return null;
            }

            @Override
            public boolean isShutdown() {
                return false;
            }

            @Override
            public boolean isTerminated() {
                return false;
            }

            @Override
            public boolean awaitTermination(long l, TimeUnit timeUnit) throws InterruptedException {
                return false;
            }

            @Override
            public <T> Future<T> submit(Callable<T> callable) {
                return null;
            }

            @Override
            public <T> Future<T> submit(Runnable runnable, T t) {
                return null;
            }

            @Override
            public Future<?> submit(Runnable runnable) {
                return null;
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> collection) throws InterruptedException {
                return null;
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit) throws InterruptedException {
                return null;
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> collection) throws InterruptedException, ExecutionException {
                return null;
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
                return null;
            }

            @Override
            public void execute(Runnable runnable) {
                executeCalled++;
            }
        }

        class TestResource extends ExecutorResource {
            public TestResource(MockExecutorService internal) {
                this.internal = internal;
            }
        }

        MockExecutorService internal = new MockExecutorService();
        TestResource tpe = new TestResource(internal);
        tpe.execute(() -> {
        });
        assertEquals(1, internal.executeCalled);
        tpe.doDispose();
        assertEquals(1, internal.shutdownCalled);
    }
}