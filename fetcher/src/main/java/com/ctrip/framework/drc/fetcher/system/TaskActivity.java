package com.ctrip.framework.drc.fetcher.system;

import java.util.concurrent.TimeUnit;

public interface TaskActivity<T, U> extends TaskSource<U> {

    boolean trySubmit(T task);

    void waitSubmit(T task) throws InterruptedException;

    boolean waitSubmit(T task, long time, TimeUnit unit) throws InterruptedException;
}
