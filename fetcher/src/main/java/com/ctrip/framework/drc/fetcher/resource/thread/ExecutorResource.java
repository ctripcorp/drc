package com.ctrip.framework.drc.fetcher.resource.thread;

import com.ctrip.framework.drc.fetcher.system.AbstractResource;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;

import java.util.concurrent.*;

/**
 * @Author Slight
 * Sep 18, 2019
 */
public class ExecutorResource extends AbstractResource implements Executor {

    protected ExecutorService internal;

    @InstanceConfig(path = "maxThreads")
    int maxThreads = 300;

    @InstanceConfig(path = "coreThreads")
    int coreThreads = 300;

    @Override
    public void doDispose() {
        internal.shutdown();
        try {
            if (!internal.awaitTermination(100, TimeUnit.MILLISECONDS)) {
                internal.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.error("UNLIKELY - interrupted when awaitTermination()");
        }
        internal = null;
    }

    @Override
    public void doInitialize() {
        internal = new ThreadPoolExecutor(
                coreThreads, maxThreads, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(maxThreads), new ThreadPoolExecutor.AbortPolicy()
        );
    }

    @Override
    public void execute(Runnable runnable) {
        internal.execute(runnable);
    }
}
