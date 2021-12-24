package com.ctrip.framework.drc.core.exception;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-02
 */
public class DelayMonitorException extends Exception {

    public DelayMonitorException(Throwable throwable) {
        super("Unknown", throwable);
    }

    public DelayMonitorException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
