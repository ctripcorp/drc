package com.ctrip.framework.drc.replicator.impl.inbound.handler;

import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.framework.drc.core.driver.command.handler.AbstractClientCommandHandler;
import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;
import com.ctrip.framework.drc.core.driver.command.impl.replicator.ComRegisterSlaveCommand;
import com.ctrip.framework.drc.core.driver.command.packet.client.RegisterSlaveCommandPacket;
import com.ctrip.framework.drc.core.driver.command.packet.server.ServerResultPackage;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.netty.commands.NettyClient;


/**
 * Created by mingdongli
 * 2019/9/18 下午7:21.
 */
public class RegisterSlaveClientCommandHandler extends AbstractClientCommandHandler<ServerResultPackage> implements CommandHandler<ServerResultPackage> {

    @Override
    public CommandFuture<ServerResultPackage> handle(ServerCommandPacket serverCommandPacket, SimpleObjectPool<NettyClient> simpleObjectPool) {
        if (serverCommandPacket instanceof RegisterSlaveCommandPacket) {
            RegisterSlaveCommandPacket registerSlaveCommandPacket = (RegisterSlaveCommandPacket) serverCommandPacket;
            ComRegisterSlaveCommand registerSlaveCommand = new ComRegisterSlaveCommand(registerSlaveCommandPacket, simpleObjectPool, scheduledExecutorService);
            return execute(registerSlaveCommand);
        }
        return new DefaultCommandFuture<>();
    }

}
