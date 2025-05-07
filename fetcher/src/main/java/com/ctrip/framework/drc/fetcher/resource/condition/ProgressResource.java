package com.ctrip.framework.drc.fetcher.resource.condition;

import com.ctrip.framework.drc.fetcher.system.AbstractResource;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author Slight
 * Sep 07, 2020
 */
public class ProgressResource extends AbstractResource implements Progress {

    private AtomicLong count = new AtomicLong(0);

    @Override
    public void tick() {
        count.addAndGet(1);
    }

    @Override
    public void clear() {
        count.set(0);
    }

    @Override
    public long get() {
        return count.get();
    }
}
