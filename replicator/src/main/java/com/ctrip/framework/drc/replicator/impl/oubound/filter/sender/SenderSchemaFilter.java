package com.ctrip.framework.drc.replicator.impl.oubound.filter.sender;

import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogSender;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.SchemaFilter;

public class SenderSchemaFilter extends SchemaFilter {
    protected final BinlogSender sender;

    public SenderSchemaFilter(SenderFilterChainContext context) {
        super(context);
        this.sender = context.getBinlogSender();
    }

    @Override
    protected void skipTransaction(OutboundLogEventContext value, long nextTransactionOffset) {
        value.setInSchemaExcludeGroup(true);
    }

    @Override
    protected boolean concern(String schema, int eventCount, boolean noRowsEvent) {
        return sender.concernSchema(schema);
    }

    @Override
    public void skipEvent(OutboundLogEventContext value) {
        // do nothing
    }

    @Override
    public void doConcern(OutboundLogEventContext value) {
        value.setSkipEvent(true);
    }
}
