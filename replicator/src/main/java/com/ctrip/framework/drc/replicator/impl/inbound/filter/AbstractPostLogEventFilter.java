package com.ctrip.framework.drc.replicator.impl.inbound.filter;

/**
 * @Author limingdong
 * @create 2020/4/24
 */
public abstract class AbstractPostLogEventFilter extends AbstractLogEventFilter  {

    @Override
    protected boolean doNext(LogEventWithGroupFlag value, boolean skip) {
        if (getSuccessor() != null) {
            return getSuccessor().doFilter(value);
        } else {
            return skip;
        }
    }
}
