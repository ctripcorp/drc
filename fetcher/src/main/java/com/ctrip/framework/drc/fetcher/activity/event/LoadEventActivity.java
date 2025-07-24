package com.ctrip.framework.drc.fetcher.activity.event;

import com.ctrip.framework.drc.fetcher.event.FetcherRowsEvent;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.fetcher.system.TaskQueueActivity;

import static com.ctrip.framework.drc.fetcher.server.FetcherServer.DEFAULT_APPLY_COUNT;

/**
 * @Author Slight
 * Jun 10, 2020
 */
public class LoadEventActivity extends TaskQueueActivity<FetcherRowsEvent, Boolean> {

    @InstanceConfig(path = "applyConcurrency")
    public int applyConcurrency = DEFAULT_APPLY_COUNT;

    @Override
    public void doStart() {
        int loadConcurrency = getLoadConcurrency(applyConcurrency);
        for (int i = 0; i < loadConcurrency; i++) {
            executor.execute(this);
        }
    }

    @Override
    public int queueSize() {
        return 1000;
    }

    @Override
    public FetcherRowsEvent doTask(FetcherRowsEvent task) {
        task.tryLoad();
        return null;
    }

    // 100 : 8
    protected static int getLoadConcurrency(int applyConcurrency) {
        int calculate = applyConcurrency * 8 / 100 + 1;
        // max 8
        if (calculate > 8) {
            return 8;
        }
        // lte 2
        return Math.max(calculate, 2);
    }
}
