package com.ctrip.framework.drc.replicator.impl.oubound.filter.sender;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.ChannelAttributeKey;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.SkipFilter;

public class SenderSkipFilter extends SkipFilter {
    private ChannelAttributeKey channelAttributeKey;

    public SenderSkipFilter(SenderFilterChainContext context) {
        super(context);
        this.channelAttributeKey = context.getChannelAttributeKey();
    }

    // todo by yongnian: 2024/4/16 这是干啥
    @Override
    protected void channelHandleEvent(LogEventType eventType) {
        if (inExcludeGroup) {
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
    protected void skipEvent(OutboundLogEventContext value) {
        // do nothing
    }
}
