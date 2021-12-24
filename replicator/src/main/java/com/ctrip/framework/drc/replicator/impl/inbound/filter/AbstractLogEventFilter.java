package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.server.common.Filter;

/**
 * Created by mingdongli
 * 2019/10/9 上午10:55.
 */
public abstract class AbstractLogEventFilter extends Filter<LogEventWithGroupFlag> {

    protected boolean doNext(LogEventWithGroupFlag value, boolean skip) {
        if (!skip && getSuccessor() != null) {
            return getSuccessor().doFilter(value);
        } else {
            return skip;
        }
    }
}
