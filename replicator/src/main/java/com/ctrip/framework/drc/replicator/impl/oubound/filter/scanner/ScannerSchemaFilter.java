package com.ctrip.framework.drc.replicator.impl.oubound.filter.scanner;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogScanner;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogSender;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.SchemaFilter;

import java.util.List;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;

/**
 * @author yongnian
 */
public class ScannerSchemaFilter extends SchemaFilter {

    private final List<BinlogSender> senders;

    private final BinlogScanner scanner;

    public ScannerSchemaFilter(ScannerFilterChainContext context) {
        super(context);
        this.senders = context.getScanner().getSenders();
        this.scanner = context.getScanner();
    }

    @Override
    public void skipEvent(OutboundLogEventContext value) {
        value.skipPosition(value.getEventSize() - eventHeaderLengthVersionGt1);
    }

    @Override
    protected boolean concern(String schema, int eventCount, boolean noRowsEvent) {
        if (noRowsEvent) {
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.outbound.noRowsEvent.filtered", schema);
            return false;
        }
        splitBigTransactionIfNeeded(schema, eventCount);
        return senders.stream().anyMatch(sender -> sender.concernSchema(schema));
    }

    private void splitBigTransactionIfNeeded(String schema, int eventCount) {
        if (eventCount >= DynamicConfig.getInstance().getScannerSplitThreshold()) {
            scanner.splitConcernSenders(schema);
        }
    }

    @Override
    protected void skipTransaction(OutboundLogEventContext value, long nextTransactionOffset) {
        value.skipPositionAfterReadEvent(nextTransactionOffset);
        value.setInExcludeGroup(false);
        scanner.getSenders().forEach(sender -> sender.refreshInExcludedGroup(value));
    }

    @Override
    public void doConcern(OutboundLogEventContext value) {
        value.setSkipEvent(false);
    }
}
