package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;

/**
 * Applier and row filter configured
 *
 * @Author limingdong
 * @create 2022/4/22
 */
public class TypeFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private ConsumeType consumeType;

    private boolean shouldFilterRows;

    public TypeFilter(ConsumeType consumeType, boolean shouldFilterRows) {
        this.consumeType = consumeType;
        this.shouldFilterRows = shouldFilterRows;
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        if (ConsumeType.Applier != consumeType || !shouldFilterRows) {
            value.setNoRowFiltered(true);
        }
        return doNext(value, value.isNoRowFiltered());
    }
}
