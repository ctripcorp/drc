package com.ctrip.framework.drc.console.monitor.cases.manager;

import com.ctrip.framework.drc.console.monitor.cases.function.MonitorCase;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-07
 */
public class AbstractMonitorCaseManager implements MonitorCaseManager {

    List<MonitorCase> monitorCases = Lists.newArrayList();

    @Override
    public void doMonitor() {
        for(MonitorCase monitorCase : monitorCases) {
            monitorCase.doMonitor();
        }
    }

    @Override
    public void addCase(MonitorCase monitorCase) {
        monitorCase.doInitialize();
        monitorCases.add(monitorCase);
    }
}
