package com.ctrip.framework.drc.console.monitor.cases.manager;

import com.ctrip.framework.drc.console.monitor.cases.function.MonitorCase;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-07
 */
public interface MonitorCaseManager {

    void doMonitor();

    void addCase(MonitorCase monitorCase);

}
