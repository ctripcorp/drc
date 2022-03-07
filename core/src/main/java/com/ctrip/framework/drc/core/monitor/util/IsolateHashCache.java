package com.ctrip.framework.drc.core.monitor.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.ForwardingCache;

/**
 * Created by jixinwang on 2022/3/2
 */

public class IsolateHashCache<K, V> extends ForwardingCache<K, V> {

    private Cache<K, V> isolateCache;

    public IsolateHashCache(int cap, int init, int concurrency) {
        super();
        this.isolateCache = CacheBuilder.newBuilder()
                .concurrencyLevel(concurrency)
                .initialCapacity(init)
                .maximumSize(cap)
                .build();
    }

    @Override
    protected Cache<K, V> delegate() {
        return isolateCache;
    }
}
