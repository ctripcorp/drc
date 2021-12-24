package com.ctrip.framework.drc.core.exception;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.exception.ErrorMessageAware;
import com.ctrip.xpipe.netty.commands.ProtocalErrorResponse;

/**
 * Created by mingdongli
 * 2019/10/30 上午9:34.
 */
public class DrcRuntimeException extends RuntimeException implements ErrorMessageAware, ProtocalErrorResponse {

    private static final long serialVersionUID = 1L;

    private ErrorMessage<?> errorMessage;

    private boolean onlyLogMessage = false;

    public DrcRuntimeException(String message){
        super(message);
    }

    public DrcRuntimeException(String message, Throwable th){
        super(message, th);
    }

    public <T extends Enum<T>> DrcRuntimeException(ErrorMessage<T> errorMessage, Throwable th){
        super(errorMessage.toString(), th);
        this.errorMessage = errorMessage;
    }

    @Override
    public ErrorMessage<?> getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(ErrorMessage<?> errorMessage) {
        this.errorMessage = errorMessage;
    }

    public boolean isOnlyLogMessage() {
        return onlyLogMessage;
    }

    public void setOnlyLogMessage(boolean onlyLogMessage) {
        this.onlyLogMessage = onlyLogMessage;
    }
}
