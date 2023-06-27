package com.ctrip.framework.drc.core.concurrent;

import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.command.DefaultRetryCommandFactory;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.concurrent.OneThreadTaskExecutor;
import com.ctrip.xpipe.utils.OsUtils;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @Author limingdong
 * @create 2021/1/21
 */
public class DrcKeyedOneThreadTaskExecutor extends KeyedOneThreadTaskExecutor {

    private final static int RETRY_INTERVAL = 2000;

    protected ScheduledExecutorService executorService = ThreadUtils.newFixedThreadScheduledPool(OsUtils.getCpuCount(), "DrcKeyedOneThreadTaskExecutor");

    public DrcKeyedOneThreadTaskExecutor(Executor executors) {
        super(executors);
    }

    protected OneThreadTaskExecutor createTaskExecutor() {
        return new OneThreadTaskExecutor(DefaultRetryCommandFactory.retryForever(executorService, getRetryInterval()), executors);
    }

    protected int getRetryInterval() {
        return RETRY_INTERVAL;
    }

    @Override
    public void destroy() throws Exception {
        executorService.shutdown();
        super.destroy();
    }
}
