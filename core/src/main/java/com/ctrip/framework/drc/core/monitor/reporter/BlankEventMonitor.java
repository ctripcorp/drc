package com.ctrip.framework.drc.core.monitor.reporter;

/**
 * @Author limingdong
 * @create 2021/12/1
 */
public class BlankEventMonitor implements EventMonitor {

    @Override
    public void logBatchEvent(String type, String name, int count, int error) {

    }

    @Override
    public void logError(Throwable cause) {

    }

    @Override
    public void logEvent(String type, String name, long count) {

    }

    @Override
    public void logEvent(String type, String name) {

    }

    @Override
    public void logAlertEvent(String simpleAlertMessage) {

    }

    @Override
    public int getOrder() {
        return 1;
    }
}
