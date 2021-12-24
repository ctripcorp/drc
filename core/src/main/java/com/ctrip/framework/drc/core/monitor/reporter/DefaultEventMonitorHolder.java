package com.ctrip.framework.drc.core.monitor.reporter;

import com.ctrip.framework.drc.core.monitor.util.ServicesUtil;

/**
 * @Author limingdong
 * @create 2021/11/30
 */
public class DefaultEventMonitorHolder {

    private static class EventMonitorHolder {
        public static final EventMonitor INSTANCE = ServicesUtil.getEventMonitorService();
    }

    public static EventMonitor getInstance() {
        return EventMonitorHolder.INSTANCE;
    }
}
