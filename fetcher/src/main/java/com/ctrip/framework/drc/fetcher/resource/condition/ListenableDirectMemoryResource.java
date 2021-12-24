package com.ctrip.framework.drc.fetcher.resource.condition;

import com.ctrip.framework.drc.fetcher.event.FetcherEventGroup;
import com.ctrip.framework.drc.fetcher.system.AbstractResource;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.core.driver.binlog.LogEventCallBack;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.google.common.collect.Sets;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author limingdong
 * @create 2020/11/26
 */
public class ListenableDirectMemoryResource extends AbstractResource implements ListenableDirectMemory {

    public static final String MIN_MEMORY_KEY = "drc.applier.min.directmemory";

    public static final String MAX_MEMORY_KEY = "drc.applier.max.directmemory";

    private AtomicLong memoryUsed = new AtomicLong(0);

    public static long MIN_MEMORY = 20 * 1000 * 1000; //20M

    public static long MAX_MEMORY = MIN_MEMORY * 5; //100M

    private Set<LogEventCallBack> listeners = Sets.newConcurrentHashSet();

    @InstanceConfig(path = "registryKey")
    public String registryKey;

    private ScheduledExecutorService scheduledExecutorService;

    private AtomicLong eventNum = new AtomicLong(0);

    protected void doInitialize() {
        scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("ListenableDirectMemoryResource-" + registryKey);
        String minSize = System.getProperty(MIN_MEMORY_KEY, String.valueOf(MIN_MEMORY));
        MIN_MEMORY = Long.parseLong(minSize);
        String maxSize = System.getProperty(MAX_MEMORY_KEY, String.valueOf(MAX_MEMORY));
        MAX_MEMORY = Long.parseLong(maxSize);
        scheduledExecutorService.scheduleAtFixedRate(() -> logger.debug("[memoryUsed]:{} for {}", memoryUsed.get(), registryKey), 0, 30, TimeUnit.SECONDS);
    }

    @Override
    public boolean tryAllocate(long size) {
        eventNum.addAndGet(1);
        return !(memoryUsed.addAndGet(size) > MAX_MEMORY && atLeastOneGroup());
    }

    @Override
    public boolean release(long size) {
        eventNum.addAndGet(-1);
        if (memoryUsed.addAndGet(-size) < MIN_MEMORY || !atLeastOneGroup()) {
            notifyListeners();
            return true;
        }
        return false;
    }

    private boolean atLeastOneGroup() {
        return eventNum.get() >= FetcherEventGroup.CAPACITY;
    }

    private synchronized void notifyListeners() {
        if (!listeners.isEmpty()) {
            for (LogEventCallBack logEventCallBack : listeners) {
                logEventCallBack.onSuccess();
            }
            listeners.clear();
        }
    }

    @Override
    public long get() {
        return memoryUsed.get();
    }

    @Override
    public synchronized void addListener(LogEventCallBack listener) {
        listeners.add(listener);
    }

    protected void doDispose() {
        scheduledExecutorService.shutdown();
    }
}
