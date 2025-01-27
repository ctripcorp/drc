package com.ctrip.framework.drc.messenger.resource.thread;

import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.fetcher.system.AbstractResource;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.xpipe.concurrent.NamedThreadFactory;

import java.util.concurrent.*;

/**
 * Created by shiruixin
 * 2025/1/22 11:55
 */
public class MqBigEventExecutorResource extends AbstractResource implements MqBigEventExecutor {
    protected ExecutorService internal;
    int maxThreads = 50;
    int coreThreads = 1;

    @InstanceConfig(path = "registryKey")
    public String registryKey;
    @InstanceConfig(path = "applyMode")
    public int applyMode;

    @Override
    protected void doInitialize() throws Exception {
        String threadPattern = "%s-%s-bigEventExecutor";
        String[] parts = registryKey.split("\\.");
        String mhaName = "";
        if (parts.length > 1) {
            mhaName = parts[1];
        }
        String threadPrefix = String.format(threadPattern, mhaName, ApplyMode.getApplyMode(applyMode).getName());
        internal = new ThreadPoolExecutor(
                coreThreads, maxThreads, 30, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(maxThreads), new NamedThreadFactory(threadPrefix),new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    @Override
    public Future<Boolean> submit(Callable<Boolean> callable) {
        return internal.submit(callable);
    }

    @Override
    protected void doDispose() throws Exception {
        internal.shutdown();
        try {
            if (!internal.awaitTermination(100, TimeUnit.MILLISECONDS)) {
                internal.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.error("UNLIKELY - interrupted when awaitTermination() in MqBigEventExecutorResource");
        }
        internal = null;
    }
}
