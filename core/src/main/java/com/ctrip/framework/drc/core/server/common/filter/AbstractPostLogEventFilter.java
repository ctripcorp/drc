package com.ctrip.framework.drc.core.server.common.filter;

/**
 * @Author limingdong
 * @create 2020/4/24
 */
public abstract class AbstractPostLogEventFilter<T> extends AbstractLogEventFilter<T>  {

    @Override
    protected boolean doNext(T value, boolean skip) {
        if (getSuccessor() != null) {
            return getSuccessor().doFilter(value);
        } else {
            return skip;
        }
    }
}
