package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.common.filter.FilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.extract.ExtractFilter;

/**
 *
 *  preFilter
 *  ConsumeTypeFilter -> TableFilter -> RowsFilter
 *
 *  postFilter
 *  SendFilter
 *
 * @Author limingdong
 * @create 2022/4/22
 */
public class OutboundFilterChainFactory implements FilterChainFactory<OutboundFilterChainContext, OutboundLogEventContext> {

    @Override
    public Filter<OutboundLogEventContext> createFilterChain(OutboundFilterChainContext context) {
        SendFilter sendFilter = new SendFilter(context.getChannel());

        TypeFilter consumeTypeFilter = new TypeFilter(context.getConsumeType(), context.shouldExtract());
        sendFilter.setSuccessor(consumeTypeFilter);

        TableFilter tableFilter = new TableFilter(context.getDataMediaConfig());
        consumeTypeFilter.setSuccessor(tableFilter);

        ExtractFilter extractFilter = new ExtractFilter(context);
        tableFilter.setSuccessor(extractFilter);

        return sendFilter;
    }
}
