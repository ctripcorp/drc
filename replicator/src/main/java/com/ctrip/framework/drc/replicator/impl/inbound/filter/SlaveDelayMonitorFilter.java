package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.replicator.impl.monitor.DefaultMonitorManager;

/**
 * @Author limingdong
 * @create 2022/12/12
 */
public class SlaveDelayMonitorFilter extends DelayMonitorFilter {

    public SlaveDelayMonitorFilter(DefaultMonitorManager delayMonitor) {
        super(delayMonitor);
    }

    @Override
    protected boolean shouldProcess(boolean filtered) {
        return !filtered;
    }
}
