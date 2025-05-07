package com.ctrip.framework.drc.core.concurrent;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author yongnian
 * @create 2024/11/1 15:10
 */
public class AllSuccessFuture implements Future<Boolean> {
    List<Future<Boolean>> futures;

    public AllSuccessFuture(List<Future<Boolean>> futures) {
        this.futures = futures;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return futures.stream().allMatch(future -> future.cancel(mayInterruptIfRunning));
    }

    @Override
    public boolean isCancelled() {
        return futures.stream().allMatch(Future::isCancelled);
    }

    @Override
    public boolean isDone() {
        return futures.stream().allMatch(Future::isDone);
    }

    @Override
    public Boolean get() throws InterruptedException, ExecutionException {
        for (Future<Boolean> dcFuture : futures) {
            if (!Boolean.TRUE.equals(dcFuture.get())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Boolean get(long timeout, TimeUnit unit) throws ExecutionException, InterruptedException, TimeoutException {
        for (Future<Boolean> dcFuture : futures) {
            if (!Boolean.TRUE.equals(dcFuture.get(timeout, unit))) {
                return false;
            }
        }
        return true;
    }
}
