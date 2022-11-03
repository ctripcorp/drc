package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.common.filter.FilterChainFactory;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;

/**
 * Created by mingdongli
 * 2019/10/9 上午10:30.
 *
 * preFilter
 * TransactionMonitorFilter -> FlagFilter -> EventTypeFilter -> UuidFilter/TransactionTableFilter -> DdlFilter -> BlackTableNameFilter
 *
 * postFilter
 * TransactionMonitorFilter(read) -> DelayMonitorFilter(read) -> PersistPostFilter(write) -> EventReleaseFilter(release)
 */
public class EventFilterChainFactory implements FilterChainFactory<InboundFilterChainContext, InboundLogEventContext> {

    public Filter<InboundLogEventContext> createFilterChain(InboundFilterChainContext context) {

        EventReleaseFilter eventReleaseFilter = new EventReleaseFilter();

        PersistPostFilter persistPostFilter = new PersistPostFilter(context.getTransactionCache());
        eventReleaseFilter.setSuccessor(persistPostFilter);

        DelayMonitorFilter delayMonitorFilter = new DelayMonitorFilter(context.getMonitorManager());
        persistPostFilter.setSuccessor(delayMonitorFilter);

        TransactionMonitorFilter transactionMonitorFilter = new TransactionMonitorFilter(context.getInboundMonitorReport());
        delayMonitorFilter.setSuccessor(transactionMonitorFilter);

        FlagFilter flagFilter = new FlagFilter();
        transactionMonitorFilter.setSuccessor(flagFilter);

        EventTypeFilter eventTypeFilter = new EventTypeFilter();
        flagFilter.setSuccessor(eventTypeFilter);

        Filter circularBreakFilter = ApplyMode.set_gtid.getType() == context.getApplyMode() ? new UuidFilter(context.getWhiteUUID()) : new TransactionTableFilter();
        eventTypeFilter.setSuccessor(circularBreakFilter);

        DdlFilter ddlFilter = new DdlFilter(context.getSchemaManager(), context.getMonitorManager());
        circularBreakFilter.setSuccessor(ddlFilter);

        BlackTableNameFilter tableNameFilter = new BlackTableNameFilter(context.getInboundMonitorReport(), context.getTableNames());
        context.registerBlackTableNameFilter(tableNameFilter);
        ddlFilter.setSuccessor(tableNameFilter);

        return eventReleaseFilter;
    }

}
