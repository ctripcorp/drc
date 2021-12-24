package com.ctrip.framework.drc.core.exception.dump;

/**
 * @Author Slight
 * Dec 01, 2019
 */
public class NetworkException extends BinlogDumpGtidException {
    public NetworkException(Throwable throwable) {
        super(throwable);
    }

    public NetworkException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public NetworkException(String message) {
        super(message);
    }
}
