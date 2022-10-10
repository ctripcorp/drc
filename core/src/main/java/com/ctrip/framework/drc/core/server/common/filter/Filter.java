package com.ctrip.framework.drc.core.server.common.filter;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mingdongli
 * 2019/10/9 上午10:07.
 */
public abstract class Filter<T> implements Releasable, Resettable {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected Filter<T> successor;

    public abstract boolean doFilter(T value);

    public Filter<T> getSuccessor() {
        return successor;
    }

    public void setSuccessor(Filter<T> successor) {
        this.successor = successor;
    }

    public void release() {
        Filter<T> successor = getSuccessor();
        while (successor != null) {
            successor.release();
            successor = successor.getSuccessor();
        }
    }

    public void reset() {
        Filter<T> successor = getSuccessor();
        while (successor != null) {
            successor.reset();
            successor = successor.getSuccessor();
        }
    }
}
