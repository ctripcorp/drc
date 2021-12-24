package com.ctrip.framework.drc.core.exception;

/**
 * Created by @author zhuYongMing on 2019/9/6.
 */
public class ByteBufUnreadableException extends DrcException {

    private static final long serialVersionUID = 1L;

    public ByteBufUnreadableException(String message) {
        super(message);
    }

    public ByteBufUnreadableException(String message, Throwable th) {
        super(message, th);
    }
}
