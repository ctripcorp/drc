package com.ctrip.framework.drc.core.filter.exception;

/**
 * Created by jixinwang on 2021/11/17
 */
public class FilterException extends DrcException {

    private static final long serialVersionUID = -7288830284122672209L;

    public FilterException(String errorCode){
        super(errorCode);
    }

    public FilterException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public FilterException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public FilterException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public FilterException(Throwable cause){
        super(cause);
    }
}
