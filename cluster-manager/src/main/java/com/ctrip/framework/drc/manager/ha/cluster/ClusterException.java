package com.ctrip.framework.drc.manager.ha.cluster;

import com.ctrip.framework.drc.core.exception.DrcException;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public class ClusterException extends DrcException {

    public ClusterException(String message) {
        super(message);
    }

    public ClusterException(String message, Throwable th) {
        super(message, th);
    }
}
