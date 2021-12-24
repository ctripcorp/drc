package com.ctrip.framework.drc.core.driver.command.packet.client;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.command.AbstractServerCommandWithHeadPacket;
import io.netty.buffer.ByteBuf;

import java.io.IOException;

import static com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND.COM_HEARTBEAT;

/**
 * Created by mingdongli
 * 2019/11/6 上午9:15.
 */
public class HeartBeatPacket extends AbstractServerCommandWithHeadPacket<HeartBeatPacket> {

    public HeartBeatPacket() {
        super(COM_HEARTBEAT.getCode());
    }

    @Override
    protected byte[] getBody() throws IOException {
        return new byte[0];
    }

    @Override
    public HeartBeatPacket read(ByteBuf byteBuf) {
        headerPacket.read(byteBuf);
        return this;
    }

    @Override
    public void write(ByteBuf byteBuf) throws IOException {
        super.write(byteBuf);
    }

    @Override
    public void write(IoCache ioCache) {

    }
}
