package com.ctrip.framework.drc.replicator.impl.oubound.handler;

import com.ctrip.framework.drc.core.driver.binlog.impl.DelayMonitorLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.ParsedDdlLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.UpdateRowsEvent;
import com.ctrip.framework.drc.core.driver.command.packet.monitor.DelayMonitorCommandPacket;
import com.ctrip.framework.drc.replicator.impl.inbound.event.ObservableLogEventHandler;
import com.ctrip.framework.drc.replicator.impl.inbound.event.ReplicatorLogEventHandler;
import com.ctrip.framework.drc.replicator.impl.monitor.DefaultMonitorManager;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.ChannelAttributeKey;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.utils.Gate;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.Attribute;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.InetSocketAddress;

/**
 * Created by jixinwang on 2021/11/11
 */
public class DelayMonitorCommandHandlerTest {

    @Test
    public void testHandle() {
        Channel channel = Mockito.mock(Channel.class);
        ChannelFuture channelFuture = Mockito.mock(ChannelFuture.class);
        Attribute attribute = Mockito.mock(Attribute.class);
        ChannelAttributeKey channelAttributeKey = Mockito.mock(ChannelAttributeKey.class);
        Gate gate = Mockito.mock(Gate.class);
        Mockito.when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 8080));
        Mockito.when(channel.closeFuture()).thenReturn(channelFuture);
        Mockito.when(channel.attr(Mockito.any())).thenReturn(attribute);
        Mockito.when(attribute.get()).thenReturn(channelAttributeKey);
        Mockito.when(channelAttributeKey.getGate()).thenReturn(gate);

        DefaultMonitorManager defaultMonitorManager = new DefaultMonitorManager("ut");
        ObservableLogEventHandler logEventHandler = new ReplicatorLogEventHandler(null, defaultMonitorManager, null);
        DelayMonitorCommandHandler delayMonitorCommandHandler = new DelayMonitorCommandHandler(logEventHandler, "test_dalcluster");
        delayMonitorCommandHandler.initialize();
        DelayMonitorCommandPacket monitorCommandPacket = new DelayMonitorCommandPacket("ntgxh", "test_dalcluster", "sha");

        NettyClient nettyClient = new DefaultNettyClient(channel);
        delayMonitorCommandHandler.handle(monitorCommandPacket, nettyClient);

        // 1) DefaultNettyClient 2) MonitorEventTask:run
        Mockito.verify(channelFuture, Mockito.atLeast(1)).addListener(Mockito.any());
        delayMonitorCommandHandler.dispose();
    }


    @Test
    public void testRelease() {
        Channel channel = Mockito.mock(Channel.class);
        ChannelFuture channelFuture = Mockito.mock(ChannelFuture.class);
        Attribute attribute = Mockito.mock(Attribute.class);
        ChannelAttributeKey channelAttributeKey = Mockito.mock(ChannelAttributeKey.class);
        Gate gate = Mockito.mock(Gate.class);
        Mockito.when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 8080));
        Mockito.when(channel.closeFuture()).thenReturn(channelFuture);
        Mockito.when(channel.attr(Mockito.any())).thenReturn(attribute);
        Mockito.when(attribute.get()).thenReturn(channelAttributeKey);
        Mockito.when(channelAttributeKey.getGate()).thenReturn(gate);




        DelayMonitorCommandHandler delayMonitorCommandHandler = new DelayMonitorCommandHandler(null,null);
        DelayMonitorCommandHandler.MonitorEventTask monitorEventTask = delayMonitorCommandHandler.new MonitorEventTask(channel, null, null);
        ByteBuf eventByteBuf = initByteBuf(dbDelayBytes);

        // release ReferenceCountedDelayMonitorLogEvent
        UpdateRowsEvent read = new UpdateRowsEvent().read(eventByteBuf);
        DelayMonitorLogEvent delayMonitorLogEvent = new DelayMonitorLogEvent("abd:1-123", read);
        delayMonitorLogEvent.retain();
        Assert.assertEquals(2, delayMonitorLogEvent.refCnt());
        monitorEventTask.release(delayMonitorLogEvent);
        Assert.assertEquals(1, delayMonitorLogEvent.refCnt());
        monitorEventTask.release(delayMonitorLogEvent);
        Assert.assertEquals(0, delayMonitorLogEvent.refCnt());

        // release log event
        ParsedDdlLogEvent logEvent = Mockito.mock(ParsedDdlLogEvent.class);
        monitorEventTask.release(logEvent);
        Mockito.verify(logEvent, Mockito.times(1)).release();
    }

    private ByteBuf initByteBuf(byte[] bytes) {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(98);
        byteBuf.writeBytes(bytes);
        return byteBuf;
    }

     byte[] dbDelayBytes = new byte[]{
            (byte) 0x51, (byte) 0x96, (byte) 0x5c, (byte) 0x65, (byte) 0x1f, (byte) 0xea, (byte) 0x0c, (byte) 0x00, (byte) 0x00, (byte) 0xd0, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xd7, (byte) 0xf1, (byte) 0xd0, (byte) 0x1f, (byte) 0x00, (byte) 0x00,
            (byte) 0x30, (byte) 0x5D, (byte) 0x01, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x02, (byte) 0x00, (byte) 0x03, (byte) 0xFF, (byte) 0xFF, (byte) 0x00, (byte) 0xD9, (byte) 0x41,
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x45, (byte) 0x00, (byte) 0x7B, (byte) 0x22, (byte) 0x64, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x6E, (byte) 0x74,
            (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x72, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22,
            (byte) 0x2C, (byte) 0x22, (byte) 0x6D, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x7A, (byte) 0x79, (byte) 0x6E, (byte) 0x5F, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5F, (byte) 0x31,
            (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x62, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x7A, (byte) 0x79, (byte) 0x6E, (byte) 0x5F, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5F,
            (byte) 0x73, (byte) 0x68, (byte) 0x61, (byte) 0x72, (byte) 0x64, (byte) 0x5F, (byte) 0x64, (byte) 0x62, (byte) 0x31, (byte) 0x36, (byte) 0x32, (byte) 0x22, (byte) 0x7D, (byte) 0x65, (byte) 0x5C, (byte) 0x96,
            (byte) 0x4F, (byte) 0x1C, (byte) 0x98, (byte) 0x00, (byte) 0xD9, (byte) 0x41, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x45, (byte) 0x00, (byte) 0x7B, (byte) 0x22,
            (byte) 0x64, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x72, (byte) 0x22, (byte) 0x3A, (byte) 0x22,
            (byte) 0x6E, (byte) 0x74, (byte) 0x67, (byte) 0x78, (byte) 0x68, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x6D, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x7A, (byte) 0x79, (byte) 0x6E, (byte) 0x5F,
            (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5F, (byte) 0x31, (byte) 0x22, (byte) 0x2C, (byte) 0x22, (byte) 0x62, (byte) 0x22, (byte) 0x3A, (byte) 0x22, (byte) 0x7A, (byte) 0x79, (byte) 0x6E,
            (byte) 0x5F, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x5F, (byte) 0x73, (byte) 0x68, (byte) 0x61, (byte) 0x72, (byte) 0x64, (byte) 0x5F, (byte) 0x64, (byte) 0x62, (byte) 0x31, (byte) 0x36,
            (byte) 0x32, (byte) 0x22, (byte) 0x7D, (byte) 0x65, (byte) 0x5C, (byte) 0x96, (byte) 0x51, (byte) 0x0E, (byte) 0xB0, (byte) 0xD4, (byte) 0xAC, (byte) 0x61, (byte) 0xAB,
    };
}
