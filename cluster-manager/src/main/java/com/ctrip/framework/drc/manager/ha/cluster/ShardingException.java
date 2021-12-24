package com.ctrip.framework.drc.manager.ha.cluster;

import com.ctrip.framework.drc.core.exception.DrcException;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public class ShardingException extends DrcException {

    public ShardingException(String message) {
        super(message);
    }

    public ShardingException(String message, Throwable th) {
        super(message, th);
    }
}
