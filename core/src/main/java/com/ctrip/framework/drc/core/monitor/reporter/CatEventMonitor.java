package com.ctrip.framework.drc.core.monitor.reporter;

import com.dianping.cat.Cat;

/**
 * @Author limingdong
 * @create 2021/11/30
 */
public class CatEventMonitor extends com.ctrip.xpipe.monitor.CatEventMonitor implements EventMonitor {

    @Override
    public int getOrder() {
        return 0;
    }

    @Override
    public void logBatchEvent(String type, String name, int count, int error) {
        Cat.logBatchEvent(type, name, count, error);
    }

    @Override
    public void logError(Throwable cause) {
        Cat.logError(cause);
    }
}
