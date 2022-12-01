package com.ctrip.framework.drc.replicator.impl.oubound.handler;

import com.ctrip.framework.drc.core.driver.binlog.HeartBeatCallBack;
import com.ctrip.framework.drc.core.driver.command.packet.client.HeartBeatResponsePacket;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.replicator.MockTest;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.ChannelAttributeKey;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.utils.Gate;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.TIME_SPAN_KEY;
import static com.ctrip.framework.drc.replicator.AllTests.validTime;
import static com.ctrip.framework.drc.replicator.impl.oubound.handler.ReplicatorMasterHandler.KEY_CLIENT;

/**
 * @Author limingdong
 * @create 2022/3/29
 */
public class HeartBeatCommandHandlerTest extends MockTest {

    private Channel channel = new EmbeddedChannel();

    private Channel delayedChannel = new DelayedEmbeddedChannel();

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        System.setProperty(TIME_SPAN_KEY, String.valueOf(validTime));
        channel.attr(KEY_CLIENT).set(new ChannelAttributeKey(new Gate("test_channel_key")));
        delayedChannel.attr(KEY_CLIENT).set(new ChannelAttributeKey(new Gate("test_channel_key")));
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
    public void testHeartBeatWithAutoReadRemove() throws InterruptedException {
        // test not remove for autoRead = 0
        HeartBeatCommandHandler heartBeatCommandHandler = new HeartBeatCommandHandler("test_key");
        heartBeatCommandHandler.setEXPIRE_TIME(100);
        heartBeatCommandHandler.initialize();
        heartBeatCommandHandler.sendHeartBeat(channel);

        HeartBeatResponsePacket heartBeatResponsePacket = new HeartBeatResponsePacket(HeartBeatCallBack.AUTO_READ_CLOSE);
        heartBeatCommandHandler.handle(heartBeatResponsePacket, new DefaultNettyClient(channel));
        TimeUnit.MILLISECONDS.sleep(100 * 2);
        Assert.assertEquals(heartBeatCommandHandler.getResponses().size(), 0 );  // AUTO_READ_CLOSE also consider time

        // remove
        heartBeatResponsePacket = new HeartBeatResponsePacket(HeartBeatCallBack.AUTO_READ);
        heartBeatCommandHandler.handle(heartBeatResponsePacket, new DefaultNettyClient(channel));
        TimeUnit.MILLISECONDS.sleep(100 * 2);
        Assert.assertEquals(heartBeatCommandHandler.getResponses().size(), 0 );

        heartBeatCommandHandler.dispose();
    }

    @Test
    public void testHeartBeatWithAutoReadNotRemove() throws InterruptedException {
        // test not remove for autoRead = 0
        HeartBeatCommandHandler heartBeatCommandHandler = new HeartBeatCommandHandler("test_key");
        heartBeatCommandHandler.setEXPIRE_TIME(100);
        heartBeatCommandHandler.initialize();
        heartBeatCommandHandler.sendHeartBeat(channel);


        HeartBeatResponsePacket heartBeatResponsePacket = new HeartBeatResponsePacket(HeartBeatCallBack.AUTO_READ_CLOSE);
        heartBeatCommandHandler.handle(heartBeatResponsePacket, new DefaultNettyClient(channel));

        // update 
        ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("test");
        scheduledExecutorService.scheduleAtFixedRate(() -> heartBeatCommandHandler.handle(heartBeatResponsePacket, new DefaultNettyClient(channel)), 50, 50, TimeUnit.MILLISECONDS);

        TimeUnit.MILLISECONDS.sleep(100 * 2);
        Assert.assertEquals(heartBeatCommandHandler.getResponses().size(), 1 );  // AUTO_READ_CLOSE also consider time, not remove

        scheduledExecutorService.shutdownNow();
        heartBeatCommandHandler.dispose();
    }

    @Test
    public void testExpire() {
        HeartBeatCommandHandler heartBeatCommandHandler = new HeartBeatCommandHandler("test_key");
        long now = System.currentTimeMillis();
        HeartBeatCommandHandler.HeartBeatContext heartBeatContext = new HeartBeatCommandHandler.HeartBeatContext(now - 1000 * 60 * 5);
        boolean res = heartBeatCommandHandler.doCheck();
        Assert.assertFalse(res);

        heartBeatCommandHandler.getResponses().putIfAbsent(channel, heartBeatContext);
        res = heartBeatCommandHandler.doCheck();
        Assert.assertTrue(res);
        Assert.assertTrue(heartBeatCommandHandler.getResponses().isEmpty());

        heartBeatContext = new HeartBeatCommandHandler.HeartBeatContext(now);
        heartBeatCommandHandler.getResponses().putIfAbsent(channel, heartBeatContext);
        res = heartBeatCommandHandler.doCheck();
        Assert.assertTrue(res);
        Assert.assertFalse(heartBeatCommandHandler.getResponses().isEmpty());
    }

    @Test
    public void testNotHeartBeat() {
        Channel channel = new EmbeddedChannel();
        channel.attr(KEY_CLIENT).set(new ChannelAttributeKey(new Gate("test_channel_key"), false));

        HeartBeatCommandHandler heartBeatCommandHandler = new HeartBeatCommandHandler("test_key");
        heartBeatCommandHandler.sendHeartBeat(channel);
        Assert.assertTrue(heartBeatCommandHandler.getResponses().isEmpty());

        long now = System.currentTimeMillis();
        HeartBeatCommandHandler.HeartBeatContext heartBeatContext = new HeartBeatCommandHandler.HeartBeatContext(now);
        heartBeatCommandHandler.getResponses().putIfAbsent(channel, heartBeatContext);
        boolean res = heartBeatCommandHandler.doCheck();
        Assert.assertTrue(res);
        Assert.assertTrue(heartBeatCommandHandler.getResponses().isEmpty());
    }

    @Test
    public void testTimeValid() {
        HeartBeatCommandHandler heartBeatCommandHandler = new HeartBeatCommandHandler("test_key");
        heartBeatCommandHandler.initialize();
        heartBeatCommandHandler.sendHeartBeat(delayedChannel);
        Assert.assertEquals(heartBeatCommandHandler.getResponses().size(), 0 );
        heartBeatCommandHandler.dispose();
    }

    class DelayedEmbeddedChannel extends EmbeddedChannel {
        @Override
        public ChannelFuture writeAndFlush(Object msg) {
            try {
                TimeUnit.MILLISECONDS.sleep(validTime + 1);
            } catch (Exception e) {
            }
            return super.writeAndFlush(msg);
        }
    }
}