package com.ctrip.framework.drc.replicator.impl.inbound.handler;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.framework.drc.core.driver.config.InstanceStatus;

/**
 * @Author limingdong
 * @create 2020/5/11
 */
public class DrcBinlogDumpGtidCommandExecutor extends BinlogDumpGtidCommandExecutor {

    public DrcBinlogDumpGtidCommandExecutor(CommandHandler commandHandler, GtidSet gtidSet, long slaveId) {
        super(commandHandler, gtidSet, slaveId);
    }

    @Override
    protected ServerCommandPacket getPacket(String queryString) {
        ApplierDumpCommandPacket dumpCommandPacket = new ApplierDumpCommandPacket(String.valueOf(slaveId), gtidSet);
        dumpCommandPacket.setConsumeType(InstanceStatus.INACTIVE.getStatus());
        return dumpCommandPacket;
    }
}
