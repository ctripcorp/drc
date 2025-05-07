package com.ctrip.framework.drc.messenger.resource.thread;

import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.fetcher.system.AbstractResource;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.messenger.utils.MqDynamicConfig;

import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Created by shiruixin
 * 2025/1/22 11:55
 */
public class MqRowEventExecutorResource extends AbstractResource implements MqRowEventExecutor {
    protected ExecutorService internal;
    int maxThreads = 50;


    @InstanceConfig(path = "registryKey")
    public String registryKey;
    @InstanceConfig(path = "applyMode")
    public int applyMode;

    @Override
    protected void doInitialize() throws Exception {
        String threadPattern = "%s-%s-rowEventExecutor";
        String[] parts = registryKey.split("\\.");
        String mhaName = "";
        if (parts.length > 1) {
            mhaName = parts[1];
        }
        String threadPrefix = String.format(threadPattern, mhaName, ApplyMode.getApplyMode(applyMode).getName());
        maxThreads = MqDynamicConfig.getInstance().getRowEventExecutorMaxThread(registryKey);
        internal = ThreadUtils.newFixedThreadPool(maxThreads, threadPrefix);
    }

    @Override
    public Future<Boolean> submit(Callable<Boolean> callable) {
        return internal.submit(callable);
    }

    @Override
    public CompletableFuture<Boolean> supplyAsync(Supplier<Boolean> supplier) {
        return CompletableFuture.supplyAsync(supplier, internal);
    }

    @Override
    public CompletableFuture<Boolean> thenApplyAsync(CompletableFuture<Boolean> future, Function<Boolean,Boolean> fn) {
        return future.thenApplyAsync(fn, internal);
    }


    @Override
    protected void doDispose() throws Exception {
        internal.shutdown();
        try {
            if (!internal.awaitTermination(100, TimeUnit.MILLISECONDS)) {
                internal.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.error("UNLIKELY - interrupted when awaitTermination() in MqRowEventExecutorResource");
        }
        internal = null;
    }
}
