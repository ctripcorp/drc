package com.ctrip.framework.drc.fetcher.activity.monitor;

import com.ctrip.framework.drc.fetcher.system.AbstractLoopActivity;
import com.ctrip.framework.drc.fetcher.system.TaskSource;

/**
 * Created by shiruixin
 * 2024/11/20 15:39
 */
public abstract class BaseWatchActivity extends AbstractLoopActivity implements TaskSource<Boolean> {
    public static class LastLWM {
        public final long lwm;
        public final long progress;
        public final long lastTimeMillis;

        public LastLWM(long lwm, long progress, long lastTimeMillis) {
            this.lwm = lwm;
            this.progress = progress;
            this.lastTimeMillis = lastTimeMillis;
        }
    }
}
