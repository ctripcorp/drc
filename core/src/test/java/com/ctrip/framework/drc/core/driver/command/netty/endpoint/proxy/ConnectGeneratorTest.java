package com.ctrip.framework.drc.core.driver.command.netty.endpoint.proxy;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

/**
 * @Author limingdong
 * @create 2021/4/14
 */
public class ConnectGeneratorTest  {

    private static final String IP = "1.1.1.1";

    private static final int PORT = 1234;

    private String routeInfo = "PROXY ROUTE PROXYTCP://127.0.0.1:80,PROXYTCP://127.0.0.2:80 PROXYTLS://127.0.0.0:443 TCP";

    private String EXPECT = "+PROXY ROUTE PROXYTLS://127.0.0.0:443 TCP://1.1.1.1:1234 \r\n";

    private ConnectGenerator connectGenerator;

    @Test
    public void generateNormal() {
        connectGenerator = new ConnectGenerator(IP, PORT, "");
        Endpoint endpoint = connectGenerator.generate();
        Assert.assertFalse(endpoint instanceof ProxyEnabled);
    }

    @Test
    public void generateProxy() {
        connectGenerator = new ConnectGenerator(IP, PORT, routeInfo);
        Endpoint endpoint = connectGenerator.generate();
        Assert.assertTrue(endpoint instanceof ProxyEnabled);
        ProxyEnabledEndpoint proxyEnabledEndpoint = (ProxyEnabledEndpoint) endpoint;
        String actualIp = proxyEnabledEndpoint.getIp();
        int actualPort = proxyEnabledEndpoint.getPort();
        Assert.assertEquals(80, actualPort);
        Assert.assertEquals("127.0.0.1", actualIp);
        ByteBuf byteBuf = proxyEnabledEndpoint.getProxyProtocol().output();
        String protocol = readString(byteBuf);
        Assert.assertEquals(EXPECT, protocol);
    }

    public static String readString(ByteBuf byteBuf) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while (byteBuf.readableBytes() > 0) {
            int item = byteBuf.readByte();
            out.write(item);
        }
        return new String(out.toByteArray());
    }
}