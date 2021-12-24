package com.ctrip.framework.drc.core.driver.command.packet.server;

import com.ctrip.framework.drc.core.driver.command.packet.HeaderPacket;
import io.netty.buffer.ByteBuf;

import java.util.Arrays;

/**
 * Created by mingdongli
 * 2019/9/8 下午4:35.
 */
public class ServerResultPackage extends HeaderPacket {

    public byte[] body;

    @Override
    public ServerResultPackage read(ByteBuf byteBuf) {
        super.read(byteBuf);
        body = getBody(byteBuf);
        return this;
    }

    public byte[] getBody() {
        return body;
    }

    @Override
    public String toString() {
        return "ServerResultPackage{" +
                "body=" + Arrays.toString(body) +
                '}';
    }
}
