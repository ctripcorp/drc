package com.ctrip.framework.drc.replicator.impl.oubound.filter.sender;

import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.common.filter.FilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.MonitorFilter;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.SendFilter;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.SenderTableNameFilter;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.extract.ExtractFilter;

/**
 * @author yongnian
 * <p>
 * preFilter
 * SenderSkipFilter -> SenderSchemaFilter -> SenderTableNameFilter -> ExtractFilter
 * <p>
 * postFilter
 * MonitorFilter -> SendFilter
 */
public class SenderFilterChainFactory implements FilterChainFactory<SenderFilterChainContext, OutboundLogEventContext> {

    @Override
    public Filter<OutboundLogEventContext> createFilterChain(SenderFilterChainContext context) {
        MonitorFilter monitorFilter = new MonitorFilter(context);

        SendFilter sendFilter = new SendFilter(context);
        monitorFilter.setSuccessor(sendFilter);

        if (ConsumeType.Replicator != context.getConsumeType()) {
            SenderSchemaFilter schemaFilter = new SenderSchemaFilter(context);
            sendFilter.setSuccessor(schemaFilter);

            SenderSkipFilter skipFilter = new SenderSkipFilter(context);
            schemaFilter.setSuccessor(skipFilter);

            SenderTableNameFilter tableNameFilter = new SenderTableNameFilter(context);
            skipFilter.setSuccessor(tableNameFilter);

            if (context.shouldExtract()) {
                ExtractFilter extractFilter = new ExtractFilter(context);
                tableNameFilter.setSuccessor(extractFilter);
            }
        } else {
            SenderSkipFilter skipFilter = new SenderSkipFilter(context);
            sendFilter.setSuccessor(skipFilter);
        }

        return monitorFilter;
    }
}
