package com.ctrip.framework.drc.console.thread;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.*;

/**
 * Created by dengquanliang
 * 2023/4/28 15:53
 */
public class ConsoleThreadFactory {

    public static ThreadFactory newThreadFactory(String threadName) {
        return new ThreadFactoryBuilder().setNameFormat(threadName + "-%d").build();
    }

    public static ExecutorService rowsFilterMetaExecutor() {
        return new ThreadPoolExecutor(20, 20, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(200), newThreadFactory("rowsFilterMeta"));
    }
}
