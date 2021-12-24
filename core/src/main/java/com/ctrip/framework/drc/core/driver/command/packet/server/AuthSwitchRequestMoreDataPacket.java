package com.ctrip.framework.drc.core.driver.command.packet.server;

import com.ctrip.framework.drc.core.driver.command.packet.HeaderPacket;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import io.netty.buffer.ByteBuf;

/**
 * Created by mingdongli
 * 2019/9/8 上午11:05.
 */
public class AuthSwitchRequestMoreDataPacket extends HeaderPacket {

    public int    status;

    public byte[] authData;

    @Override
    public AuthSwitchRequestMoreDataPacket read(ByteBuf byteBuf) {
        super.read(byteBuf);
        fromBytes(getBody(byteBuf));
        return this;
    }

    public void fromBytes(byte[] data) {
        int index = 0;
        // 1. read status
        status = data[index];
        index += 1;
        authData = ByteHelper.readNullTerminatedBytes(data, index);
    }

}
