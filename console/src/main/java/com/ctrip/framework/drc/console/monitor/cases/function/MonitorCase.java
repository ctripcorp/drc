package com.ctrip.framework.drc.console.monitor.cases.function;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-07
 */
public interface MonitorCase {
    void doInitialize();
    void doMonitor();
}
