package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import java.util.concurrent.Callable;

import static com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable.DDL_LOGGER;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class RetryTask<V> implements Callable {

    public static final int MAX_RETRY = 5;

    private NamedCallable<V> callable;

    private int actualMaxRetry = MAX_RETRY;

    public RetryTask(NamedCallable<V> callable) {
        this(callable, MAX_RETRY);
    }

    public RetryTask(NamedCallable<V> callable, int retryTime) {
        this.callable = callable;
        this.actualMaxRetry = retryTime;
    }

    @Override
    public V call() {
        int retryTime = 0;
        do {
            try {
                V res =  callable.call();
                DDL_LOGGER.info("{} success with retryTime {}", callable.name(), retryTime);
                return res;
            } catch (Throwable t) {
                retryTime++;
                callable.afterException(t);
            }
        } while (retryTime <= actualMaxRetry);

        callable.afterFail();
        return null;
    }
}
