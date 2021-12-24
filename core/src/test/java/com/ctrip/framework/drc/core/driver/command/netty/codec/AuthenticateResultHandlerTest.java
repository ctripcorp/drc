package com.ctrip.framework.drc.core.driver.command.netty.codec;

import com.ctrip.framework.drc.core.MockTest;
import com.ctrip.framework.drc.core.driver.command.netty.NettyClientFactory;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.NettyClientWithEndpoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.util.Attribute;
import org.apache.tomcat.util.buf.HexUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @Author limingdong
 * @create 2020/11/25
 */
public class AuthenticateResultHandlerTest extends MockTest {

    private AuthenticateResultHandler authenticateResultHandler = new AuthenticateResultHandler();

    private ByteBuf successResponse;

    private ByteBuf failResponse;

    private ByteBuf authResponse;

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

    private CountDownLatch countDownLatch;

    private int count;

    @Before
    public void setUp() {
        super.initMocks();
        successResponse = getSuccessResponse();
        failResponse = getFailResponse();
        authResponse = getAuthSuccess();
        count = new Random().nextInt(100);
        countDownLatch = new CountDownLatch(count);
    }

    @After
    public void tearDown() {
        successResponse.release();
        failResponse.release();
        authResponse.release();
    }

    @Test
    public void channelRead0Success() throws Exception {
        when(channelHandlerContext.channel()).thenReturn(channel);
        when(channelHandlerContext.pipeline()).thenReturn(pipeline);
        when(channel.attr(NettyClientFactory.KEY_CLIENT)).thenReturn(attributeKey);
        when(attributeKey.get()).thenReturn(nettyClientWithEndpoint);
        when(nettyClientWithEndpoint.getCountDownLatch()).thenReturn(countDownLatch);
        authenticateResultHandler.channelRead0(channelHandlerContext, successResponse);
        long left = nettyClientWithEndpoint.getCountDownLatch().getCount();
        Assert.assertEquals(left, count - 1);
    }

    @Test
    public void channelRead0Switch() throws Exception {
        when(channelHandlerContext.channel()).thenReturn(channel);
        when(channelHandlerContext.pipeline()).thenReturn(pipeline);
        when(channel.attr(NettyClientFactory.KEY_CLIENT)).thenReturn(attributeKey);
        when(attributeKey.get()).thenReturn(nettyClientWithEndpoint);
        when(nettyClientWithEndpoint.getCountDownLatch()).thenReturn(countDownLatch);
        when(nettyClientWithEndpoint.endpoint()).thenReturn(endpoint);
        when(endpoint.getPassword()).thenReturn("1qaz2wsx0p;/9ol.");
        authenticateResultHandler.channelRead0(channelHandlerContext, failResponse);
        long left = nettyClientWithEndpoint.getCountDownLatch().getCount();
        Assert.assertEquals(left, count);
        verify(channel, times(1)).writeAndFlush(anyObject());  //switch res

        authenticateResultHandler.channelRead0(channelHandlerContext, authResponse); //auth success
        authenticateResultHandler.channelRead0(channelHandlerContext, successResponse); //success
        left = nettyClientWithEndpoint.getCountDownLatch().getCount();
        Assert.assertEquals(left, count - 1);
    }

    private ByteBuf getSuccessResponse() {
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(16);
        String hexString = "07 00 00 02 00 00 00 02 00 00 00";
        hexString = hexString.replaceAll(" ", "");
        final byte[] bytes = HexUtils.fromHexString(hexString);
        byteBuf.writeBytes(bytes);
        return byteBuf;
    }

    private ByteBuf getFailResponse() {
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(64);
        byte[] b = new byte[]{44, 0, 0, 1, -2, 99, 97, 99, 104, 105, 110, 103, 95, 115, 104, 97, 50, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0, 112, 55, 26, 15, 91, 30, 57, 54, 1, 108, 116, 106, 120, 84, 32, 59, 121, 125, 76, 42, 0};
        byteBuf.writeBytes(b);
        return byteBuf;
    }

    private ByteBuf getAuthSuccess() {
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(8);
        byte[] b = new byte[]{2, 0, 0, 3, 1, 3};
        byteBuf.writeBytes(b);
        return byteBuf;
    }
}