package com.ctrip.framework.drc.replicator.impl.oubound.handler.binlog;

import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.AbstractBinlogScannerManager;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogSender;
import io.netty.channel.Channel;

import java.util.List;

public class LocalBinlogScannerManager extends AbstractBinlogScannerManager {


    public LocalBinlogScannerManager() {
        super("local.mha");
    }

    public LocalBinlogScanner createScannerInner(List<BinlogSender> binlogSenders) throws Exception {
        LocalBinlogScanner localBinlogScanner = new LocalBinlogScanner(this, binlogSenders);
        return localBinlogScanner;
    }

    @Override
    public BinlogSender createSenderInner(Channel channel, ApplierDumpCommandPacket dumpCommandPacket) throws Exception {
        LocalBinlogSender sender = new LocalBinlogSender(channel, dumpCommandPacket);
        sender.initialize();
        sender.start();
        return sender;
    }


}
