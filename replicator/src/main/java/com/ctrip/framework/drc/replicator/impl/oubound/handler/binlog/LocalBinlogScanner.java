package com.ctrip.framework.drc.replicator.impl.oubound.handler.binlog;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.AbstractBinlogScanner;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.AbstractBinlogScannerManager;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogScanner;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogSender;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;
import com.ctrip.framework.drc.replicator.store.manager.file.BinlogPosition;

import java.util.List;

public class LocalBinlogScanner extends AbstractBinlogScanner {

    int fakeGtid = 0;
    String uuid = "zyn";
    BinlogPosition binlogPosition;

    public LocalBinlogScanner(AbstractBinlogScannerManager manager, List<BinlogSender> localBinlogSenders) {
        super(manager, localBinlogSenders);
        this.consumeName = "[Scanner] [" + excludedSet + "]";
    }

    @Override
    public BinlogScanner cloneScanner(List<BinlogSender> senders) {
        return new LocalBinlogScanner(this.manager, senders);
    }

    @Override
    public String getCurrentSendingFileName() {
        return "rbinlog.0000000001";
    }

    @Override
    protected void setFileChannel(OutboundLogEventContext context) {
    }

    @Override
    protected void fileRoll() {

    }

    @Override
    public BinlogPosition getBinlogPosition() {
        return binlogPosition;
    }

    @Override
    protected void preSend(OutboundLogEventContext context) {
        super.preSend(context);
        excludedSet.add(context.getGtid());
    }

    @Override
    protected boolean isConcern(OutboundLogEventContext context) {
        return !new GtidSet(context.getGtid()).isContainedWithin(excludedSet);
    }

    @Override
    public void readNextEvent(OutboundLogEventContext context) {
        fakeGtid++;
        String gtid = String.format("%s:%d", uuid, fakeGtid);

        logger.info("{} send {}", this, gtid);
        context.setGtid(gtid);
        context.setEventType(LogEventType.gtid_log_event);
    }

    @Override
    protected void readFilePosition(OutboundLogEventContext context) {
        context.reset(Long.MAX_VALUE);
    }

    public void setBinlogPosition(BinlogPosition binlogPosition) {
        this.binlogPosition = binlogPosition;
    }

}
