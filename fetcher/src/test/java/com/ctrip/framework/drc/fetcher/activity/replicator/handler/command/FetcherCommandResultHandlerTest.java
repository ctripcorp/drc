package com.ctrip.framework.drc.fetcher.activity.replicator.handler.command;

import com.ctrip.framework.drc.core.monitor.reporter.CatEventMonitor;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.fetcher.MockTest;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.Attribute;
import io.netty.util.concurrent.GenericFutureListener;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;

import java.net.SocketAddress;

import static com.ctrip.framework.drc.core.driver.command.netty.NettyClientFactory.KEY_LAST_PROCESS_TIME;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.APPLIER_PROCESS_EVENT_MAX_IDLE_TIMEOUT;
import static org.mockito.Mockito.mockStatic;

/**
 * Created by shiruixin
 * 2025/10/13 14:23
 */
public class FetcherCommandResultHandlerTest extends MockTest {

    private FetcherCommandResultHandler handler;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private Channel channel;

    @Mock
    private ChannelConfig channelConfig;

    @Mock
    private ChannelFuture channelFuture;

    @Mock
    private ChannelPromise channelPromise;

    @Mock
    private Attribute<Long> lastProcessTimeAttr;

    @Mock
    private SocketAddress remoteAddress;

    @Mock
    private CatEventMonitor catEventMonitor;

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        handler = new FetcherCommandResultHandler();
        
        when(ctx.channel()).thenReturn(channel);
        when(channel.config()).thenReturn(channelConfig);
        when(channel.remoteAddress()).thenReturn(remoteAddress);
        when(remoteAddress.toString()).thenReturn("127.0.0.1:3306");
        when(channel.attr(KEY_LAST_PROCESS_TIME)).thenReturn(lastProcessTimeAttr);
        when(channel.writeAndFlush(any(ByteBuf.class))).thenReturn(channelFuture);
        when(channelFuture.addListener(any(GenericFutureListener.class))).thenReturn(channelFuture);
        when(channelFuture.isSuccess()).thenReturn(true);
    }

    class IdleStateEventMock extends IdleStateEvent{
        protected IdleStateEventMock(IdleState state, boolean first) {
            super(state, first);
        }
    }

    @Test
    public void testUserEventTriggered_READER_IDLE_AutoReadTrue() throws Exception {
        // Given
        IdleStateEvent event = new IdleStateEventMock(IdleState.READER_IDLE, true);
        when(channelConfig.isAutoRead()).thenReturn(true);

        try (MockedStatic<DefaultEventMonitorHolder> mockedEventMonitor = mockStatic(DefaultEventMonitorHolder.class)) {
            mockedEventMonitor.when(DefaultEventMonitorHolder::getInstance).thenReturn(catEventMonitor);

            // When
            handler.userEventTriggered(ctx, event);

            // Then
            verify(catEventMonitor).logEvent("DRC.replicator.mysql.readidle", "127.0.0.1:3306");
            verify(ctx).close();
            verify(ctx).fireUserEventTriggered(event);
        }
    }

    @Test
    public void testUserEventTriggered_READER_IDLE_AutoReadFalse() throws Exception {
        // Given
        IdleStateEvent event = new IdleStateEventMock(IdleState.READER_IDLE, true);
        when(channelConfig.isAutoRead()).thenReturn(false);

        // When
        handler.userEventTriggered(ctx, event);

        // Then
        verify(ctx, never()).close();
        verify(ctx).fireUserEventTriggered(event);
    }

    @Test
    public void testUserEventTriggered_WRITER_IDLE_WithValidLastProcessTime() throws Exception {
        // Given
        IdleStateEvent event = new IdleStateEventMock(IdleState.WRITER_IDLE, true);
        long currentTime = System.currentTimeMillis();
        when(lastProcessTimeAttr.get()).thenReturn(currentTime - 1000); // 1 second ago
        when(channelConfig.isAutoRead()).thenReturn(true);

        try (MockedStatic<DefaultEventMonitorHolder> mockedEventMonitor = mockStatic(DefaultEventMonitorHolder.class)) {
            mockedEventMonitor.when(DefaultEventMonitorHolder::getInstance).thenReturn(catEventMonitor);

            // When
            handler.userEventTriggered(ctx, event);

            // Then
            verify(catEventMonitor).logEvent("DRC.applier.writeidle.heartbeat", "127.0.0.1:3306");
            verify(channel).writeAndFlush(any(ByteBuf.class));
            verify(ctx).fireUserEventTriggered(event);
        }
    }

    @Test
    public void testUserEventTriggered_WRITER_IDLE_WithNullLastProcessTime() throws Exception {
        // Given
        IdleStateEvent event = new IdleStateEventMock(IdleState.WRITER_IDLE, true);
        when(lastProcessTimeAttr.get()).thenReturn(null);
        when(channelConfig.isAutoRead()).thenReturn(true);

        try (MockedStatic<DefaultEventMonitorHolder> mockedEventMonitor = mockStatic(DefaultEventMonitorHolder.class)) {
            mockedEventMonitor.when(DefaultEventMonitorHolder::getInstance).thenReturn(catEventMonitor);

            // When
            handler.userEventTriggered(ctx, event);

            // Then
            verify(catEventMonitor).logEvent("DRC.applier.writeidle.heartbeat.skip", "127.0.0.1:3306");
            verify(channel, never()).writeAndFlush(any(ByteBuf.class));
            verify(ctx).fireUserEventTriggered(event);
        }
    }

    @Test
    public void testUserEventTriggered_WRITER_IDLE_WithExpiredLastProcessTime() throws Exception {
        // Given
        IdleStateEvent event = new IdleStateEventMock(IdleState.WRITER_IDLE, true);
        long expiredTime = System.currentTimeMillis() - APPLIER_PROCESS_EVENT_MAX_IDLE_TIMEOUT - 1000; // Exceeded timeout
        when(lastProcessTimeAttr.get()).thenReturn(expiredTime);
        when(channelConfig.isAutoRead()).thenReturn(true);

        try (MockedStatic<DefaultEventMonitorHolder> mockedEventMonitor = mockStatic(DefaultEventMonitorHolder.class)) {
            mockedEventMonitor.when(DefaultEventMonitorHolder::getInstance).thenReturn(catEventMonitor);

            // When
            handler.userEventTriggered(ctx, event);

            // Then
            verify(catEventMonitor).logEvent("DRC.applier.writeidle.heartbeat.skip", "127.0.0.1:3306");
            verify(channel, never()).writeAndFlush(any(ByteBuf.class));
            verify(ctx).fireUserEventTriggered(event);
        }
    }

    @Test
    public void testUserEventTriggered_HandleWriterIdle_Success() throws Exception {
        // Given
        IdleStateEvent event = new IdleStateEventMock(IdleState.WRITER_IDLE, true);
        long currentTime = System.currentTimeMillis();
        when(lastProcessTimeAttr.get()).thenReturn(currentTime - 1000);
        when(channelFuture.isSuccess()).thenReturn(true);
        when(channelConfig.isAutoRead()).thenReturn(true);

        try (MockedStatic<DefaultEventMonitorHolder> mockedEventMonitor = mockStatic(DefaultEventMonitorHolder.class)) {
            mockedEventMonitor.when(DefaultEventMonitorHolder::getInstance).thenReturn(catEventMonitor);

            // When
            handler.userEventTriggered(ctx, event);

            // Then
            verify(channel).writeAndFlush(any(ByteBuf.class));
            verify(channelFuture).addListener(any(GenericFutureListener.class));
            verify(ctx, never()).close();
        }
    }

    @Test
    public void testUserEventTriggered_HandleWriterIdle_Failure() throws Exception {
        // Given
        IdleStateEvent event = new IdleStateEventMock(IdleState.WRITER_IDLE, true);
        long currentTime = System.currentTimeMillis();
        when(lastProcessTimeAttr.get()).thenReturn(currentTime - 1000);
        when(channelFuture.isSuccess()).thenReturn(false);
        when(channelConfig.isAutoRead()).thenReturn(true);

        try (MockedStatic<DefaultEventMonitorHolder> mockedEventMonitor = mockStatic(DefaultEventMonitorHolder.class)) {
            DefaultEventMonitorHolder mockHolder = mock(DefaultEventMonitorHolder.class);
            mockedEventMonitor.when(DefaultEventMonitorHolder::getInstance).thenReturn(catEventMonitor);

            // When
            handler.userEventTriggered(ctx, event);

            // Then
            verify(channel).writeAndFlush(any(ByteBuf.class));
            verify(channelFuture).addListener(any(GenericFutureListener.class));
            // Note: The close() call happens in the listener, which is harder to test directly
        }
    }

    @Test
    public void testUserEventTriggered_HandleWriterIdle_Exception() throws Exception {
        // Given
        IdleStateEvent event = new IdleStateEventMock(IdleState.WRITER_IDLE, true);
        long currentTime = System.currentTimeMillis();
        when(lastProcessTimeAttr.get()).thenReturn(currentTime - 1000);
        when(channel.writeAndFlush(any(ByteBuf.class))).thenThrow(new RuntimeException("Test exception"));
        when(channelConfig.isAutoRead()).thenReturn(true);

        try (MockedStatic<DefaultEventMonitorHolder> mockedEventMonitor = mockStatic(DefaultEventMonitorHolder.class)) {
            DefaultEventMonitorHolder mockHolder = mock(DefaultEventMonitorHolder.class);
            mockedEventMonitor.when(DefaultEventMonitorHolder::getInstance).thenReturn(catEventMonitor);

            // When
            handler.userEventTriggered(ctx, event);

            // Then
            verify(ctx).close();
            verify(ctx).fireUserEventTriggered(event);
        }
    }

    @Test
    public void testUserEventTriggered_AllIdleStateEvent() throws Exception {
        // Given
        IdleStateEvent event = new IdleStateEventMock(IdleState.ALL_IDLE, true);

        // When
        handler.userEventTriggered(ctx, event);

        // Then
        verify(ctx).fireUserEventTriggered(event);
    }
}