package com.ctrip.framework.drc.core.driver.binlog.impl;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-16
 */
public class DelayMonitorLogEvent extends ReferenceCountedDelayMonitorLogEvent {

    public DelayMonitorLogEvent() {
    }

    public DelayMonitorLogEvent(String gtid, UpdateRowsEvent updateRowsEvent) {
        super(gtid, updateRowsEvent);
    }

    @Override
    public void release() {
        super.release();
        if (this.updateRowsEvent != null) {
            this.updateRowsEvent.release();
        }
    }
}
