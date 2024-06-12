package com.ctrip.framework.drc.replicator.impl.oubound.filter.scanner;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.ChannelAttributeKey;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.SkipFilter;

import java.util.List;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;

/**
 * @author yongnian
 */
public class ScannerSkipFilter extends SkipFilter {
    private final List<ChannelAttributeKey> channelAttributeKeyList;

    public ScannerSkipFilter(ScannerFilterChainContext context) {
        super(context);
        this.channelAttributeKeyList = context.getChannelAttributeKey();
    }


    @Override
    protected void channelHandleEvent(LogEventType eventType) {
        for (ChannelAttributeKey channelAttributeKey : channelAttributeKeyList) {
            if (inExcludeGroup) {
                // scanner skip all
                channelAttributeKey.handleEvent(false);
            }
        }
    }


    @Override
    protected void skipTransaction(OutboundLogEventContext value, long nextTransactionOffset) {
        value.skipPositionAfterReadEvent(nextTransactionOffset);
        inExcludeGroup = false;
    }


    @Override
    protected void skipEvent(OutboundLogEventContext value) {
        value.skipPosition(value.getEventSize() - eventHeaderLengthVersionGt1);
    }
}
