package com.ctrip.framework.drc.core.driver.command.impl.replicator;

import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.command.packet.client.RegisterSlaveCommandPacket;
import com.ctrip.framework.drc.core.driver.command.packet.server.ErrorPacket;
import com.ctrip.framework.drc.core.driver.command.packet.server.ServerResultPackage;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by mingdongli
 * 2019/9/10 下午4:20.
 */
public class ComRegisterSlaveCommand extends AbstractMySQLCommand<ServerResultPackage> {

    public ComRegisterSlaveCommand(RegisterSlaveCommandPacket commandPacket, SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
        super(commandPacket, clientPool, scheduled);
    }

    @Override
    protected ServerResultPackage doReceiveResponse(Channel channel, ByteBuf byteBuf) throws Exception {
        ServerResultPackage serverResultPackage = new ServerResultPackage();
        serverResultPackage.read(byteBuf);
        byte[] body = serverResultPackage.getBody();
        assert body != null;
        if (body[0] < 0) {
            if (body[0] == -1) {
                ErrorPacket errorPacket = new ErrorPacket();
                errorPacket.fromBytes(body);
                throw new IOException("Error When doing Register slave:" + errorPacket.message);
            } else {
                throw new IOException("unpexpected packet with field_count=" + body[0]);
            }
        }
        return serverResultPackage;
    }

    @Override
    public SERVER_COMMAND getCommandType() {
        return SERVER_COMMAND.COM_REGISTER_SLAVE;
    }

}
