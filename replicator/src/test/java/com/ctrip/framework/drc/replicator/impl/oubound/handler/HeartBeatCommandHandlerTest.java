package com.ctrip.framework.drc.replicator.impl.oubound.handler;

import com.ctrip.framework.drc.core.driver.binlog.HeartBeatCallBack;
import com.ctrip.framework.drc.core.driver.command.packet.client.HeartBeatResponsePacket;
import com.ctrip.framework.drc.replicator.MockTest;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * @Author limingdong
 * @create 2022/3/29
 */
public class HeartBeatCommandHandlerTest extends MockTest {

    private Channel channel = new EmbeddedChannel();

    @Before
    public void setUp() throws Exception {
        super.initMocks();
    }

    @Test
    public void testHeartBeat() throws InterruptedException {
        // test not remove for CONNECTION_IDLE_TIMEOUT_SECOND
        HeartBeatCommandHandler heartBeatCommandHandler = new HeartBeatCommandHandler("test_key");
        heartBeatCommandHandler.initialize();
        heartBeatCommandHandler.sendHeartBeat(channel);
        TimeUnit.MILLISECONDS.sleep(100);
        Assert.assertEquals(heartBeatCommandHandler.getResponses().size(), 1 );
        heartBeatCommandHandler.dispose();

        // test for remove
        heartBeatCommandHandler = new HeartBeatCommandHandler("test_key");
        heartBeatCommandHandler.setEXPIRE_TIME(100);
        heartBeatCommandHandler.initialize();
        heartBeatCommandHandler.sendHeartBeat(channel);
        TimeUnit.MILLISECONDS.sleep(100 * 2);
        Assert.assertEquals(heartBeatCommandHandler.getResponses().size(), 0 );
        heartBeatCommandHandler.dispose();
    }

    @Test
    public void testHeartBeatWithAutoRead() throws InterruptedException {
        // test not remove for autoRead = 0
        HeartBeatCommandHandler heartBeatCommandHandler = new HeartBeatCommandHandler("test_key");
        heartBeatCommandHandler.setEXPIRE_TIME(100);
        heartBeatCommandHandler.initialize();
        heartBeatCommandHandler.sendHeartBeat(channel);

        HeartBeatResponsePacket heartBeatResponsePacket = new HeartBeatResponsePacket(HeartBeatCallBack.AUTO_READ_CLOSE);
        heartBeatCommandHandler.handle(heartBeatResponsePacket, new DefaultNettyClient(channel));
        TimeUnit.MILLISECONDS.sleep(100 * 2);
        Assert.assertEquals(heartBeatCommandHandler.getResponses().size(), 1 );

        // remove
        heartBeatResponsePacket = new HeartBeatResponsePacket(HeartBeatCallBack.AUTO_READ);
        heartBeatCommandHandler.handle(heartBeatResponsePacket, new DefaultNettyClient(channel));
        TimeUnit.MILLISECONDS.sleep(100 * 2);
        Assert.assertEquals(heartBeatCommandHandler.getResponses().size(), 0 );

        heartBeatCommandHandler.dispose();
    }
}