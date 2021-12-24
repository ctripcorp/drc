package com.ctrip.framework.drc.manager.ha.rest;

import com.ctrip.framework.drc.core.exception.DrcRuntimeException;

/**
 * @Author limingdong
 * @create 2020/5/30
 */
public class CircularForwardException extends DrcRuntimeException {

    public CircularForwardException(String message) {
        super(message);
    }
}
