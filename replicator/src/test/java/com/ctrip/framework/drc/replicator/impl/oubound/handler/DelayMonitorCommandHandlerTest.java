package com.ctrip.framework.drc.replicator.impl.oubound.handler;

import com.ctrip.framework.drc.core.driver.command.packet.monitor.DelayMonitorCommandPacket;
import com.ctrip.framework.drc.replicator.impl.inbound.event.ObservableLogEventHandler;
import com.ctrip.framework.drc.replicator.impl.inbound.event.ReplicatorLogEventHandler;
import com.ctrip.framework.drc.replicator.impl.monitor.DefaultMonitorManager;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClient;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

/**
 * Created by jixinwang on 2021/11/11
 */
public class DelayMonitorCommandHandlerTest {

    @Test
    public void handle() {
        DefaultMonitorManager defaultMonitorManager = new DefaultMonitorManager();
        ObservableLogEventHandler logEventHandler = new ReplicatorLogEventHandler(null, defaultMonitorManager, null);
        DelayMonitorCommandHandler delayMonitorCommandHandler = new DelayMonitorCommandHandler(logEventHandler, "test_dalcluster");
        DelayMonitorCommandPacket monitorCommandPacket = new DelayMonitorCommandPacket("ntgxh", "test_dalcluster");

        NettyClient nettyClient = new DefaultNettyClient(new EmbeddedChannel());
        delayMonitorCommandHandler.handle(monitorCommandPacket, nettyClient);

        EmbeddedChannel embeddedChannel = new EmbeddedChannel();
        ByteBuf buf = Unpooled.buffer();
        ByteBuf input = buf.duplicate();
        embeddedChannel.writeInbound(input.retain());
        input.retain();
    }
}
