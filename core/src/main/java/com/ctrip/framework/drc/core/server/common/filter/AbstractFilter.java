package com.ctrip.framework.drc.core.server.common.filter;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public abstract class AbstractFilter<T> extends Filter<T> {

    protected boolean doNext(T value, boolean skip) {
        if (!skip && getSuccessor() != null) {
            return getSuccessor().doFilter(value);
        } else {
            return skip;
        }
    }
}

