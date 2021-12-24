package com.ctrip.framework.drc.core.driver.command;

import com.ctrip.framework.drc.core.driver.command.packet.HeaderPacket;
import io.netty.buffer.ByteBuf;

import java.io.IOException;

/**
 * Created by mingdongli
 * 2019/9/10 上午8:55.
 */
public abstract class AbstractServerCommandWithHeadPacket<V extends ServerCommandPacket> implements ServerCommandPacket<V> {

    protected HeaderPacket headerPacket = new HeaderPacket();

    private byte command;

    public AbstractServerCommandWithHeadPacket(byte command) {
        this.command = command;
    }

    @Override
    public void write(ByteBuf byteBuf) throws IOException {

        byte[] body = getBody(); //一个command字节的长度 + 数据长度
        headerPacket.setPacketBodyLength(body.length);
        headerPacket.setPacketSequenceNumber((byte) 0);
        byteBuf.writeBytes(headerPacket.toBytes());
        byteBuf.writeBytes(body);
    }

    protected byte[] getBody(ByteBuf in) {
        return headerPacket.getBody(in);
    }

    protected abstract byte[] getBody() throws IOException;

    public HeaderPacket getHeaderPacket() {
        return headerPacket;
    }

    public void setHeaderPacket(HeaderPacket headerPacket) {
        this.headerPacket = headerPacket;
    }

    public byte getCommand() {
        return command;
    }

    public void setCommand(byte command) {
        this.command = command;
    }
}
