package com.ctrip.framework.drc.replicator.impl.oubound.filter.sender;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogSender;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.ChannelAttributeKey;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.SkipFilter;

public class SenderSkipFilter extends SkipFilter {
    private ChannelAttributeKey channelAttributeKey;
    private final BinlogSender binlogSender;

    public SenderSkipFilter(SenderFilterChainContext context) {
        super(context);
        this.channelAttributeKey = context.getChannelAttributeKey();
        this.binlogSender = context.getBinlogSender();
    }

    @Override
    protected void channelHandleEvent(OutboundLogEventContext value, LogEventType eventType) {
        if (value.isInGtidExcludeGroup()) {
            channelAttributeKey.handleEvent(false);
        } else {
            channelAttributeKey.handleEvent(true);
            logGtid(previousGtid, eventType);
        }
    }

    @Override
    protected void skipTransaction(OutboundLogEventContext value, long nextTransactionOffset) {
        // do nothing
    }


    @Override
    protected GtidSet getExcludedGtidSet() {
        return binlogSender.getGtidSet();
    }

    @Override
    protected void skipEvent(OutboundLogEventContext value) {
        // do nothing
    }
}
