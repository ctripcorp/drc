package com.ctrip.framework.drc.core.exception;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.exception.ErrorMessageAware;

/**
 * Created by @author zhuYongMing on 2019/9/6.
 */
public class DrcException extends Exception implements ErrorMessageAware {

    private static final long serialVersionUID = 1L;
    private ErrorMessage<?>  errorMessage;
    private boolean onlyLogMessage = false;

    public DrcException(String message){
        super(message);
    }

    public DrcException(String message, Throwable th){
        super(message, th);
    }

    public <T extends Enum<T>> DrcException(ErrorMessage<T> errorMessage, Throwable th){
        super(errorMessage.toString(), th);
        this.errorMessage = errorMessage;
    }

    @Override
    public ErrorMessage<?> getErrorMessage() {
        return errorMessage;
    }

    public boolean isOnlyLogMessage() {
        return onlyLogMessage;
    }

    public void setOnlyLogMessage(boolean onlyLogMessage) {
        this.onlyLogMessage = onlyLogMessage;
    }
}
