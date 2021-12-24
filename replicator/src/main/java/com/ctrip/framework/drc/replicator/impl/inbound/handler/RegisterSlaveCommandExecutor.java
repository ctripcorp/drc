package com.ctrip.framework.drc.replicator.impl.inbound.handler;

import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.framework.drc.core.driver.command.packet.client.RegisterSlaveCommandPacket;
import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;
import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * Created by mingdongli
 * 2019/9/19 下午11:51.
 */
public class RegisterSlaveCommandExecutor extends AbstractCommandExecutor implements CommandExecutor {

    private Endpoint endpoint;

    private long slaveId;

    public RegisterSlaveCommandExecutor(CommandHandler commandHandler, Endpoint endpoint, long slaveId) {
        super(commandHandler);
        this.endpoint = endpoint;
        this.slaveId = slaveId;
    }

    @Override
    protected ServerCommandPacket getPacket(String queryString) {
        return new RegisterSlaveCommandPacket(endpoint.getHost(), endpoint.getPort(), endpoint.getUser(), endpoint.getPassword(), slaveId);
    }

}
