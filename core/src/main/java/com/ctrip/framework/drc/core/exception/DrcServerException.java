package com.ctrip.framework.drc.core.exception;

import com.ctrip.xpipe.exception.ErrorMessage;

/**
 * Created by mingdongli
 * 2019/10/30 上午9:33.
 */
public class DrcServerException extends DrcRuntimeException {

    public DrcServerException(String message) {
        super(message);
    }

    public DrcServerException(String message, Throwable th) {
        super(message, th);
    }

    public <T extends Enum<T>> DrcServerException(ErrorMessage<T> errorMessage, Throwable th) {
        super(errorMessage, th);
    }
}
