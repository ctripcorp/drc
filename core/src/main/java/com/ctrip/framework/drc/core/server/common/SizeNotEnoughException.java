package com.ctrip.framework.drc.core.server.common;

/**
 * Created by jixinwang on 2023/10/11
 */
public class SizeNotEnoughException extends RuntimeException {

    public SizeNotEnoughException(String message) {
        super(message);
    }
}
