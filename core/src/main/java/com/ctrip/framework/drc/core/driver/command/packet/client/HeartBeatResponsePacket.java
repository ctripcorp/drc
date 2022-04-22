package com.ctrip.framework.drc.core.driver.command.packet.client;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.command.AbstractServerCommandWithHeadPacket;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import io.netty.buffer.ByteBuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.ctrip.framework.drc.core.driver.binlog.HeartBeatCallBack.AUTO_READ;
import static com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND.COM_HEARTBEAT_RESPONSE;

/**
 * @Author limingdong
 * @create 2022/3/29
 */
public class HeartBeatResponsePacket extends AbstractServerCommandWithHeadPacket<HeartBeatResponsePacket> {

    private int autoRead;  // true=1, false=0

    public HeartBeatResponsePacket() {
        this(AUTO_READ);
    }

    public HeartBeatResponsePacket(int autoRead) {
        super(COM_HEARTBEAT_RESPONSE.getCode());
        this.autoRead = autoRead;
    }

    public HeartBeatResponsePacket(byte command) { super(command); }

    @Override
    protected byte[] getBody() {
        return toBytes();
    }

    @Override
    public HeartBeatResponsePacket read(ByteBuf byteBuf) {
        headerPacket.read(byteBuf);
        fromBytes(getBody(byteBuf));
        return this;
    }

    @Override
    public void write(ByteBuf byteBuf) throws IOException {
        super.write(byteBuf);
    }

    @Override
    public void write(IoCache ioCache) {

    }

    private void fromBytes(byte[] data) {
        int index = 0;
        // 1. command
        setCommand(data[index]);
        index++;

        autoRead = ByteHelper.readUnsignedShortLittleEndian(data, index);
    }

    public byte[] toBytes() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // 0. [1] write command number
        out.write(getCommand());
        ByteHelper.writeUnsignedShortLittleEndian(autoRead, out);
        return out.toByteArray();
    }

    public int getAutoRead() {
        return autoRead;
    }
}
