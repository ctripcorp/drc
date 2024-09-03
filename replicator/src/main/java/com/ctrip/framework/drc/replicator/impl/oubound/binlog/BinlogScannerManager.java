package com.ctrip.framework.drc.replicator.impl.oubound.binlog;

import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.xpipe.api.lifecycle.Disposable;
import io.netty.channel.Channel;

import java.util.List;

/**
 * @author yongnian
 */
public interface BinlogScannerManager extends Disposable {
    BinlogSender startSender(Channel channel, ApplierDumpCommandPacket dumpCommandPacket) throws Exception;

    void startScanner(BinlogScanner binlogScanner);

    void createScanner(List<BinlogSender> binlogSenders) throws Exception;

    void tryMergeScanner(BinlogScanner scanner);

    void removeScanner(BinlogScanner scanner, boolean removeSender);

    boolean isScannerEmpty();

    List<BinlogScanner> getScanners();

    String getRegistryKey();
}
