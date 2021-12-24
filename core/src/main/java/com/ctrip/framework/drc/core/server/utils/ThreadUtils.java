package com.ctrip.framework.drc.core.server.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * Created by mingdongli
 * 2019/10/27 上午11:41.
 */
public class ThreadUtils {

    private static final Logger logger = LoggerFactory.getLogger(ThreadUtils.class);

    private static final int QUEUE_SIZE = 128;

    public static ExecutorService newThreadExecutor(int corePoolSize, int maximumPoolSize, int queueSize, String processName) {
        return new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue(queueSize), newThreadFactory(processName), new ThreadUtils.RejectedExecutionHandlerImpl());
    }

    public static ExecutorService newThreadExecutor(int corePoolSize, int maximumPoolSize, String processName) {
        return newThreadExecutor(corePoolSize, maximumPoolSize, processName, new ThreadUtils.RejectedExecutionHandlerImpl());
    }

    public static ExecutorService newThreadExecutor(int corePoolSize, int maximumPoolSize, String processName, RejectedExecutionHandler handler) {
        return new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue(QUEUE_SIZE), newThreadFactory(processName), handler);
    }

    public static ExecutorService newSingleThreadExecutor(String processName) {
        return Executors.newSingleThreadExecutor(newThreadFactory(processName));
    }

    public static ExecutorService newFixedThreadPool(int qty, String processName) {
        return Executors.newFixedThreadPool(qty, newThreadFactory(processName));
    }

    public static ScheduledExecutorService newSingleThreadScheduledExecutor(String processName) {
        return Executors.newSingleThreadScheduledExecutor(newThreadFactory(processName));
    }

    public static ScheduledExecutorService newScheduledExecutor(int qty, String processName) {
        return Executors.newScheduledThreadPool(qty, newThreadFactory(processName));
    }

    public static ScheduledExecutorService newFixedThreadScheduledPool(int qty, String processName) {
        return Executors.newScheduledThreadPool(qty, newThreadFactory(processName));
    }

    public static ExecutorService newCachedThreadPool(String processName) {
        return Executors.newCachedThreadPool(newThreadFactory(processName));
    }

    public static ThreadFactory newThreadFactory(String processName) {
        return new ThreadFactoryBuilder()
                .setNameFormat(processName + "-%d")
                .setDaemon(true)
                .build();
    }

    public static String getProcessName(Class<?> clazz) {
        if ( clazz.isAnonymousClass() )
        {
            return getProcessName(clazz.getEnclosingClass());
        }
        return clazz.getSimpleName();
    }

    public static class RejectedExecutionHandlerImpl implements RejectedExecutionHandler {
        public RejectedExecutionHandlerImpl() {
        }

        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            ThreadUtils.logger.info("{} is rejected", r.toString());
        }
    }
}
