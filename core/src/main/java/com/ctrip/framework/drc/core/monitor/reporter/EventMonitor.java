package com.ctrip.framework.drc.core.monitor.reporter;

import com.ctrip.xpipe.api.lifecycle.Ordered;

/**
 * @Author limingdong
 * @create 2021/11/30
 */
public interface EventMonitor extends com.ctrip.xpipe.api.monitor.EventMonitor, Ordered {

    void logBatchEvent(String type, String name, int count, int error);

    void logError(Throwable cause);
}
