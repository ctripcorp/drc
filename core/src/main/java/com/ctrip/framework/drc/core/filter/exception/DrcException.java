package com.ctrip.framework.drc.core.filter.exception;

import org.apache.commons.lang.exception.NestableRuntimeException;

/**
 * Created by jixinwang on 2021/11/17
 */
public class DrcException extends NestableRuntimeException {

    private static final long serialVersionUID = -654893533794556357L;

    public DrcException(String errorCode){
        super(errorCode);
    }

    public DrcException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public DrcException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public DrcException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public DrcException(Throwable cause){
        super(cause);
    }

    public Throwable fillInStackTrace() {
        return this;
    }
}
