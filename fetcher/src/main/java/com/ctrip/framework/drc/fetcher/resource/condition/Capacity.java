package com.ctrip.framework.drc.fetcher.resource.condition;

import java.util.concurrent.TimeUnit;

/**
 * @Author Slight
 * Aug 11, 2020
 */
public interface Capacity {

    boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException;
    void release();
}
