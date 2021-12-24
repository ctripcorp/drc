package com.ctrip.framework.drc.core.driver.command.packet.client;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.command.packet.HeaderPacket;
import io.netty.buffer.ByteBuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by mingdongli
 * 2019/9/8 上午11:24.
 */
public class AuthSwitchResponsePacket extends HeaderPacket {

    public byte[] authData;

    @Override
    public void write(IoCache ioCache) {

    }

    @Override
    public void write(ByteBuf byteBuf) throws IOException {
        byte[] body = toBytes();
        setPacketBodyLength(body.length);
        byteBuf.writeBytes(super.toBytes());
        byteBuf.writeBytes(body);
    }

    @Override
    public AuthSwitchResponsePacket read(ByteBuf byteBuf) {
        super.read(byteBuf);
        fromBytes(getBody(byteBuf));
        return this;
    }


    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(authData);
        return out.toByteArray();
    }

    public void fromBytes(byte[] data) {
        authData = data;
    }

    public byte[] getAuthData() {
        return authData;
    }

    public void setAuthData(byte[] authData) {
        this.authData = authData;
    }
}
