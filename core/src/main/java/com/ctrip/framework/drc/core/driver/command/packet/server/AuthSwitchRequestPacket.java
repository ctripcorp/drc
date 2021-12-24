package com.ctrip.framework.drc.core.driver.command.packet.server;

import com.ctrip.framework.drc.core.driver.command.packet.HeaderPacket;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import io.netty.buffer.ByteBuf;

/**
 * Created by mingdongli
 * 2019/9/8 上午11:14.
 */
public class AuthSwitchRequestPacket extends HeaderPacket {

    public int status;

    public String authName;

    public byte[] authData;

    @Override
    public AuthSwitchRequestPacket read(ByteBuf byteBuf) {
        super.read(byteBuf);
        fromBytes(getBody(byteBuf));
        return this;
    }

    public void fromBytes(byte[] data) {
        int index = 0;
        // 1. read status
        status = data[index];
        index += 1;
        byte[] authName = ByteHelper.readNullTerminatedBytes(data, index);
        this.authName = new String(authName);
        index += authName.length + 1;
        authData = ByteHelper.readNullTerminatedBytes(data, index);
    }
}
