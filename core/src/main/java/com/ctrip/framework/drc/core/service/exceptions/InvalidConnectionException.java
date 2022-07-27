package com.ctrip.framework.drc.core.service.exceptions;

/**
 * Created by jixinwang on 2022/7/21
 */
public class InvalidConnectionException extends RuntimeException {

    public InvalidConnectionException(String msg) {
        super(msg);
    }
}
