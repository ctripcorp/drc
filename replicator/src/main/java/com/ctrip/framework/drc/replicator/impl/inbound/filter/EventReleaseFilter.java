package com.ctrip.framework.drc.replicator.impl.inbound.filter;

/**
 * post filter
 * @Author limingdong
 * @create 2020/4/24

 */
public class EventReleaseFilter extends AbstractPostLogEventFilter {

    @Override
    public boolean doFilter(LogEventWithGroupFlag value) {

        boolean filtered = doNext(value, value.isInExcludeGroup());  //post filter
        value.releaseEvent();
        return filtered;

    }

}
