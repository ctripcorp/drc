package com.ctrip.framework.drc.fetcher.resource.condition;

import com.ctrip.framework.drc.fetcher.system.AbstractResource;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * @Author Slight
 * Aug 11, 2020
 */
public class CapacityResource extends AbstractResource implements Capacity {

    private Semaphore semaphore;

    @InstanceConfig(path = "capacity")
    public int capacity = 5000;

    @Override
    protected void doInitialize() throws Exception {
        semaphore = new Semaphore(capacity);
    }

    @Override
    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        return semaphore.tryAcquire(timeout, unit);
    }

    @Override
    public void release() {
        semaphore.release();
    }
}
