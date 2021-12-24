package com.ctrip.framework.drc.core.driver.command.netty.endpoint.proxy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.Charset;

/**
 * @Author limingdong
 * @create 2021/4/9
 */
public class DrcProxyProtocol implements ProxyProtocol {

    private String protocol;

    public DrcProxyProtocol(String protocol) {
        this.protocol = protocol;
    }

    @Override
    public ByteBuf output() {
        String connectProxy = String.format("%s%s %s %s \r\n", PLUS, KEY_WORD, ROUTE, protocol);
        return Unpooled.wrappedBuffer(connectProxy.getBytes(Charset.forName("UTF-8")));
    }

}
