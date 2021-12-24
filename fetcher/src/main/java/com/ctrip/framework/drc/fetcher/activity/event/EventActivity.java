package com.ctrip.framework.drc.fetcher.activity.event;

import com.ctrip.framework.drc.fetcher.system.TaskQueueActivity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author Slight
 * Jul 07, 2020
 */
public abstract class EventActivity<F, T> extends TaskQueueActivity<F, T> {

    protected static final Logger loggerTL = LoggerFactory.getLogger("TRX LINK");

    private final String className = getClass().getSimpleName();
    private final String namespace = className.substring(
            0,
            className.lastIndexOf("Activity")
    ).toLowerCase();

    @Override
    public String toString() {
        return namespace;
    }

    @Override
    public String namespace() {
        return namespace;
    }

}
