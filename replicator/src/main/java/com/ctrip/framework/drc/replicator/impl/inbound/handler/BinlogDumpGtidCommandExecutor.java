package com.ctrip.framework.drc.replicator.impl.inbound.handler;

import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.framework.drc.core.driver.command.packet.client.BinlogDumpGtidCommandPacket;
import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;

/**
 * Created by mingdongli
 * 2019/9/19 下午11:56.
 */
public class BinlogDumpGtidCommandExecutor extends AbstractCommandExecutor implements CommandExecutor {

    protected GtidSet gtidSet;

    protected long slaveId;

    public BinlogDumpGtidCommandExecutor(CommandHandler commandHandler, GtidSet gtidSet, long slaveId) {
        super(commandHandler);
        this.gtidSet = gtidSet;
        this.slaveId = slaveId;
    }

    @Override
    protected ServerCommandPacket getPacket(String queryString) {
        return new BinlogDumpGtidCommandPacket(slaveId, gtidSet);
    }

}
