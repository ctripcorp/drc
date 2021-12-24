package com.ctrip.framework.drc.core.exception.dump;

/**
 * @Author Slight
 * Dec 01, 2019
 */
public class EventConvertException extends BinlogDumpGtidException {
    public EventConvertException(Throwable throwable) {
        super(throwable);
    }

    public EventConvertException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public EventConvertException(String message) {
        super(message);
    }
}
