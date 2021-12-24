package com.ctrip.framework.drc.core.exception;

import com.ctrip.xpipe.exception.ErrorMessage;

/**
 * Created by mingdongli
 * 2019/10/23 下午4:02.
 */
public class LogEventException extends DrcRuntimeException {

    public LogEventException(String message) {
        super(message);
    }

    public LogEventException(String message, Throwable th) {
        super(message, th);
    }

    public <T extends Enum<T>> LogEventException(ErrorMessage<T> errorMessage, Throwable th) {
        super(errorMessage, th);
    }
}
