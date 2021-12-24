package com.ctrip.framework.drc.fetcher.activity.event;

import com.ctrip.framework.drc.fetcher.event.FetcherRowsEvent;
import com.ctrip.framework.drc.fetcher.system.TaskQueueActivity;

/**
 * @Author Slight
 * Jun 10, 2020
 */
public class LoadEventActivity extends TaskQueueActivity<FetcherRowsEvent, Boolean> {

    @Override
    public void doStart() {
        for (int i = 0; i < 8; i++) {
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
}
