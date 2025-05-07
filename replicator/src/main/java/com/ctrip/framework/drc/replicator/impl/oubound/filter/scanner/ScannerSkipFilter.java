package com.ctrip.framework.drc.replicator.impl.oubound.filter.scanner;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogScanner;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.ChannelAttributeKey;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.SkipFilter;

import java.util.List;

/**
 * @author yongnian
 */
public class ScannerSkipFilter extends SkipFilter {
    private final List<ChannelAttributeKey> channelAttributeKeyList;
    private final BinlogScanner scanner;

    public ScannerSkipFilter(ScannerFilterChainContext context) {
        super(context);
        this.channelAttributeKeyList = context.getChannelAttributeKey();
        this.scanner = context.getScanner();
    }


    @Override
    protected void channelHandleEvent(OutboundLogEventContext value, LogEventType eventType) {
        if (value.isInGtidExcludeGroup()) {
            for (ChannelAttributeKey channelAttributeKey : channelAttributeKeyList) {
                // scanner skip all
                channelAttributeKey.handleEvent(false);
            }
        }
    }


    @Override
    protected void skipTransaction(OutboundLogEventContext value, long nextTransactionOffset) {
        value.skipPositionAfterReadEvent(nextTransactionOffset);
        value.setInGtidExcludeGroup(false);
        value.setInSchemaExcludeGroup(false);
        scanner.getSenders().forEach(sender -> sender.refreshInExcludedGroup(value));
    }


    @Override
    protected void skipEvent(OutboundLogEventContext value) {
        if (value.getLogEvent() == null) {
            value.skipEvent();
        }
    }

    @Override
    protected GtidSet getExcludedGtidSet() {
        return scanner.getGtidSet();
    }
}
