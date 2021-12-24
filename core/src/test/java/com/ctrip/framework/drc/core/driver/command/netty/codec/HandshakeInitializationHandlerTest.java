package com.ctrip.framework.drc.core.driver.command.netty.codec;

import com.ctrip.framework.drc.core.MockTest;
import com.ctrip.framework.drc.core.driver.binlog.impl.BytesUtil;
import com.ctrip.framework.drc.core.driver.command.netty.NettyClientFactory;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.NettyClientWithEndpoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.util.Attribute;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/**
 * @Author limingdong
 * @create 2021/4/8
 */
public class HandshakeInitializationHandlerTest extends MockTest {

    private HandshakeInitializationHandler handshakeInitializationHandler = new HandshakeInitializationHandler();

    private ByteBuf handshake;

    @Mock
    private ChannelHandlerContext channelHandlerContext;

    @Mock
    private Channel channel;

    @Mock
    private Attribute<NettyClientWithEndpoint> attributeKey;

    @Mock
    private NettyClientWithEndpoint nettyClientWithEndpoint;

    @Mock
    private Endpoint endpoint;

    @Mock
    private ChannelPipeline pipeline;

    @Before
    public void setUp() {
        super.initMocks();
        handshake = initHandshakeByteBuf();
    }

    @After
    public void tearDown() {
        handshake.release();
    }

    @Test
    public void testChannelRead0() throws Exception {
        when(channelHandlerContext.channel()).thenReturn(channel);
        when(channelHandlerContext.pipeline()).thenReturn(pipeline);
        when(channel.attr(NettyClientFactory.KEY_CLIENT)).thenReturn(attributeKey);
        when(attributeKey.get()).thenReturn(nettyClientWithEndpoint);
        when(nettyClientWithEndpoint.endpoint()).thenReturn(endpoint);
        when(endpoint.getUser()).thenReturn("user");
        when(endpoint.getPassword()).thenReturn("passward");
        when(endpoint.getScheme()).thenReturn("schema");
        handshakeInitializationHandler.channelRead0(channelHandlerContext, handshake);
        verify(channel, times(1 )).writeAndFlush(anyObject());
    }

    private ByteBuf initHandshakeByteBuf() {
        String hexString = "4E 00 00 00 0A 35 2E 37 2E 32 37 2D 6C 6F 67 00 04 00 00 00 57 65 58 66 41 20 68 7D 00 FF FF 08 02 00 FF C1 15 00 00 00 00 00 00 00 00 00 00 6D 52 60 29 53 65 1B 78 25 2C 44 7E 00 6D 79 73 71 6C 5F 6E 61 74 69 76 65 5F 70 61 73 73 77 6F 72 64 00";

        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(200);
        final byte[] bytes = BytesUtil.toBytesFromHexString(hexString);
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

}