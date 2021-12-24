package com.ctrip.framework.drc.monitor.performance;

import com.google.common.util.concurrent.RateLimiter;

/**
 * Created by mingdongli
 * 2019/10/13 下午12:30.
 */
public class RateLimiterService {

    private RateLimiter rateLimiter;

    public RateLimiterService() {
        this(1000 / 3);
    }

    public RateLimiterService(int size) {
        rateLimiter = RateLimiter.create(size);
    }

    public boolean tryAcquire(){
        return rateLimiter.tryAcquire();
    }
}
