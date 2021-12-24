package com.ctrip.framework.drc.core.driver.binlog.manager.exception;

/**
 * @Author limingdong
 * @create 2020/3/29
 */
public class DdlException extends Exception {

    public DdlException(Throwable throwable) {
        super("ddl error", throwable);
    }

    public DdlException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
