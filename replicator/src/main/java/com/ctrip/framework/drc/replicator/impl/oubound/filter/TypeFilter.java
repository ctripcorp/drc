package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.enums.RowFilterType;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;

/**
 * Applier and row filter configured
 *
 * @Author limingdong
 * @create 2022/4/22
 */
public class TypeFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private ConsumeType consumeType;

    private RowFilterType filterType;

    public TypeFilter(ConsumeType consumeType, RowFilterType filterType) {
        this.consumeType = consumeType;
        this.filterType = filterType;
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        if (ConsumeType.Applier != consumeType || RowFilterType.None == filterType) {
            value.setSkip(true);
        }
        return doNext(value, value.isSkip());
    }
}
