package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.common.filter.FilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.row.RowsFilter;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class OutboundFilterChainFactory implements FilterChainFactory<OutboundFilterChainContext, OutboundLogEventContext> {

    @Override
    public Filter<OutboundLogEventContext> createFilterChain(OutboundFilterChainContext context) {
        SendFilter sendFilter = new SendFilter(context.getChannel());

        ConsumeTypeFilter consumeTypeFilter = new ConsumeTypeFilter(context.getConsumeType());
        sendFilter.setSuccessor(consumeTypeFilter);

        EventTypeFilter eventTypeFilter = new EventTypeFilter();
        consumeTypeFilter.setSuccessor(eventTypeFilter);

        TableFilter tableFilter = new TableFilter(context.getTableMapWithinTransaction());
        eventTypeFilter.setSuccessor(tableFilter);


        RowsFilter lineFilter = new RowsFilter(context.getFilterContext());
        eventTypeFilter.setSuccessor(lineFilter);

        return sendFilter;
    }
}
