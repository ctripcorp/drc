package com.ctrip.framework.drc.core.utils;

import com.ctrip.xpipe.utils.Gate;

/**
 * can schedule close (schedule canceled by open operation)
 *
 * @author: yongnian
 * @create: 2024/6/6 20:30
 */
public class ScheduleCloseGate extends Gate {
    public ScheduleCloseGate(String name) {
        super(name);
    }

    private boolean scheduleClose = false;


    public synchronized void scheduleClose() {
        scheduleClose = true;
    }

    public synchronized boolean closeIfScheduled() {
        if (scheduleClose) {
            super.close();
            scheduleClose = false;
            return true;
        }
        return false;
    }

    @Override
    public synchronized void open() {
        scheduleClose = false;
        super.open();
    }

}
