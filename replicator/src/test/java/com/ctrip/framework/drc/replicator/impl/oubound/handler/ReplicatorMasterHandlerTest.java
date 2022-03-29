package com.ctrip.framework.drc.replicator.impl.oubound.handler;

import com.ctrip.framework.drc.replicator.MockTest;
import com.ctrip.xpipe.utils.Gate;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.ChannelInputShutdownReadComplete;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.Attribute;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.net.SocketAddress;

import static com.ctrip.framework.drc.replicator.impl.oubound.handler.ReplicatorMasterHandler.KEY_CLIENT;
import static io.netty.handler.timeout.IdleState.WRITER_IDLE;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class ReplicatorMasterHandlerTest extends MockTest {

    @InjectMocks
    private ReplicatorMasterHandler masterHandler;

    @Mock
    private ChannelHandlerContext channelHandlerContext;

    @Mock
    private Channel channel;

    @Mock
    private CommandHandlerManager commandHandlerManager;

    @Mock
    private Attribute attribute;

    @Mock
    private Gate gate;

    @Mock
    private IdleStateEvent idleStateEvent;

    private ChannelInputShutdownReadComplete inputShutdownReadComplete = ChannelInputShutdownReadComplete.INSTANCE;

    @Mock
    private ChannelFuture future;

    @Mock
    private SocketAddress socketAddress;

    @Before
    public void setUp() {
        super.initMocks();
    }

    @Test
    public void testChannelWritabilityChanged() {
        when(channelHandlerContext.channel()).thenReturn(channel);
        when(channel.attr(KEY_CLIENT)).thenReturn(attribute);
        when(attribute.get()).thenReturn(gate);

        when(channel.isWritable()).thenReturn(true);
        masterHandler.channelWritabilityChanged(channelHandlerContext);
        verify(gate, times(1)).open();

        when(channel.isWritable()).thenReturn(false);
        masterHandler.channelWritabilityChanged(channelHandlerContext);
        verify(gate, times(1)).close();
    }

    @Test
    public void testUserEventTriggered() {
        when(channelHandlerContext.channel()).thenReturn(channel);
        when(channelHandlerContext.writeAndFlush(any(ByteBuf.class))).thenReturn(future);
        when(idleStateEvent.state()).thenReturn(WRITER_IDLE);
        when(attribute.get()).thenReturn(gate);
        when(channel.remoteAddress()).thenReturn(socketAddress);

        when(channel.isWritable()).thenReturn(false);
        masterHandler.userEventTriggered(channelHandlerContext, idleStateEvent);
        verify(channelHandlerContext, times(0)).writeAndFlush(any(ByteBuf.class));

        when(channel.isWritable()).thenReturn(true);
        masterHandler.userEventTriggered(channelHandlerContext, idleStateEvent);
        verify(commandHandlerManager, times(1)).sendHeartBeat(any(Channel.class));

        masterHandler.userEventTriggered(channelHandlerContext, inputShutdownReadComplete);
        verify(channelHandlerContext, times(1)).close();

    }

}