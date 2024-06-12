package com.ctrip.framework.drc.replicator.impl.oubound.handler.binlog;

import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.framework.drc.replicator.impl.oubound.ReplicatorException;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.AbstractBinlogScannerManager;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogSender;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.ApplierRegisterCommandHandler;
import io.netty.channel.Channel;

import java.util.List;

/**
 * @author yongnian
 */
public class DefaultBinlogScannerManager extends AbstractBinlogScannerManager {
    protected ApplierRegisterCommandHandler applierRegisterCommandHandler;

    public DefaultBinlogScannerManager(ApplierRegisterCommandHandler applierRegisterCommandHandler) {
        super(applierRegisterCommandHandler.getReplicatorName());
        this.applierRegisterCommandHandler = applierRegisterCommandHandler;
    }

    @Override
    public DefaultBinlogSender createSenderInner(Channel channel, ApplierDumpCommandPacket dumpCommandPacket) throws Exception {
        DefaultBinlogSender sender = new DefaultBinlogSender(applierRegisterCommandHandler, channel, dumpCommandPacket);
        sender.initialize();
        sender.start();
        return sender;
    }

    public DefaultBinlogScanner createScannerInner(List<BinlogSender> binlogSenders) throws Exception {
        try {
            return new DefaultBinlogScanner(applierRegisterCommandHandler, this, binlogSenders);
        } catch (Throwable e) {
            ResultCode resultCode;
            if (e instanceof ReplicatorException) {
                resultCode = ((ReplicatorException) e).getResultCode();
            } else {
                resultCode = ResultCode.UNKNOWN_ERROR;
            }
            DefaultBinlogScanner.sendResult(resultCode, binlogSenders);
            throw e;
        }
    }
}
