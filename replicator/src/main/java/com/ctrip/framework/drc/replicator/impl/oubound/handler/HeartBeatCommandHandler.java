package com.ctrip.framework.drc.replicator.impl.oubound.handler;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.HeartBeatCallBack;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcHeartbeatLogEvent;
import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;
import com.ctrip.framework.drc.core.driver.command.handler.HeartBeatHandler;
import com.ctrip.framework.drc.core.driver.command.packet.client.HeartBeatResponsePacket;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.replicator.container.config.HeartBeatConfiguration;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONNECTION_IDLE_TIMEOUT_SECOND;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.HEARTBEAT_LOGGER;

/**
 * if Applier or Console close autoRead, it may not send heartbeat response, so don't close channel
 * @Author limingdong
 * @create 2022/3/29
 */
public class HeartBeatCommandHandler extends AbstractServerCommandHandler implements CommandHandler, HeartBeatHandler {

    private long EXPIRE_TIME = CONNECTION_IDLE_TIMEOUT_SECOND * 1000 * 2;  // 60 seconds

    private String registerKey;

    private ScheduledExecutorService heartBeatExecutorService;

    private ConcurrentMap<Channel, HeartBeatContext> responses = Maps.newConcurrentMap();

    private HeartBeatConfiguration heartBeatConfiguration = HeartBeatConfiguration.getInstance();

    public HeartBeatCommandHandler(String registerKey) {
        this.registerKey = registerKey;
    }

    @Override
    public void handle(ServerCommandPacket serverCommandPacket, NettyClient nettyClient) {
        HeartBeatResponsePacket heartBeatResponsePacket = (HeartBeatResponsePacket) serverCommandPacket;
        int autoRead = heartBeatResponsePacket.getAutoRead();
        if (autoRead != HeartBeatCallBack.AUTO_READ) {
            updateHeartBeatContext(nettyClient.channel());
        } else {
            removeHeartBeatContext(nettyClient.channel());
        }
    }

    @Override
    public SERVER_COMMAND getCommandType() {
        return SERVER_COMMAND.COM_HEARTBEAT_RESPONSE;
    }

    @Override
    public void dispose() {
        responses.clear();
        heartBeatExecutorService.shutdown();
    }

    @Override
    public void initialize() {
        heartBeatExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("HeartBeatResponse-" + registerKey);
        heartBeatExecutorService.scheduleWithFixedDelay(() -> {
            try {
                if (!heartBeatConfiguration.gray(registerKey)) {
                    responses.clear();
                    HEARTBEAT_LOGGER.info("[HeartBeat] scheduled service return due to not gray for {}", registerKey);
                    return;
                }
                if (responses.isEmpty()) {
                    HEARTBEAT_LOGGER.info("[HeartBeat] scheduled service return due to empty");
                    return;
                }
                Map<Channel, HeartBeatContext> copy = Maps.newHashMap(responses);
                for (Map.Entry<Channel, HeartBeatContext> entry : copy.entrySet()) {
                    Channel channel = entry.getKey();
                    HEARTBEAT_LOGGER.debug("[HeartBeat] check {}:{}", channel, entry.getValue());
                    if (expired(entry.getValue())) {
                        removeHeartBeatContext(channel);
                        channel.close();
                        HEARTBEAT_LOGGER.warn("[HeartBeat] expired for {} and close channel", channel);
                        DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.heartbeat.expire", registerKey + ":" + channel.remoteAddress());
                    }
                }
            } catch (Throwable t) {
                HEARTBEAT_LOGGER.error("[HeartBeat] check error");
            }
        }, 0, EXPIRE_TIME / 6, TimeUnit.MILLISECONDS);
    }

    @Override
    public void sendHeartBeat(Channel channel) {
        if (!heartBeatConfiguration.getHeartBeatSwitch()) {
            HEARTBEAT_LOGGER.info("[HeartBeat] switch off for {}", registerKey);
            return;
        }
        DrcHeartbeatLogEvent drcHeartbeatLogEvent = new DrcHeartbeatLogEvent(0);
        drcHeartbeatLogEvent.write(new IoCache() {
            @Override
            public void write(byte[] data) {
            }

            @Override
            public void write(Collection<ByteBuf> byteBufs) {
                int index = 0;
                for (ByteBuf byteBuf : byteBufs) {
                    byteBuf.readerIndex(0);
                    ChannelFuture future = channel.writeAndFlush(byteBuf);
                    if (index == 0) {  // listen for header
                        future.addListener((GenericFutureListener) f -> {
                            if (!f.isSuccess()) {
                                removeHeartBeatContext(channel);
                                HEARTBEAT_LOGGER.error("[Remove] {} due to sending drcHeartbeatLogEvent error", channel);
                            } else {
                                HeartBeatContext prev = null;
                                if (!responses.containsKey(channel)) {
                                    HeartBeatContext context = newHeartBeatContext();
                                    prev = responses.putIfAbsent(channel, context);
                                }
                                HEARTBEAT_LOGGER.info("[Send] heartbeat to {}:{}, prev:{}", channel, responses.get(channel), prev);
                            }
                        });
                        index++;
                    }
                }
            }

            @Override
            public void write(Collection<ByteBuf> byteBuf, boolean isDdl) {
            }

            @Override
            public void write(LogEvent logEvent) {
            }
        });
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.heartbeat.request", registerKey + ":" + channel.remoteAddress());
    }

    private void removeHeartBeatContext(Channel channel) {
        HeartBeatContext heartBeatContext = responses.remove(channel);
        HEARTBEAT_LOGGER.info("[Receive] heartbeat for {}:{}", channel, heartBeatContext);
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.heartbeat.response", registerKey + ":" + channel.remoteAddress());
    }

    // update time when AUTO_READ_CLOSE
    private void updateHeartBeatContext(Channel channel) {
        HeartBeatContext heartBeatContext = MapUtils.getOrCreate(responses, channel, () -> newHeartBeatContext());
        heartBeatContext.setAutoRead(HeartBeatCallBack.AUTO_READ_CLOSE);
        heartBeatContext.setTime(System.currentTimeMillis());
        HEARTBEAT_LOGGER.info("[Update] heartbeat for {}:{}", channel, heartBeatContext);
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.heartbeat.autoread", registerKey + ":" + channel.remoteAddress());
    }

    private HeartBeatContext newHeartBeatContext() {
        return new HeartBeatContext(System.currentTimeMillis());
    }

    private boolean expired(HeartBeatContext heartBeatContext) {
        return System.currentTimeMillis() - heartBeatContext.getTime() > EXPIRE_TIME;
    }

    @VisibleForTesting
    public void setEXPIRE_TIME(long expireTime) {
        EXPIRE_TIME = expireTime;
    }

    @VisibleForTesting
    public ConcurrentMap<Channel, HeartBeatContext> getResponses() {
        return responses;
    }

    private static class HeartBeatContext {

        private long time;

        private int autoRead = HeartBeatCallBack.AUTO_READ;

        public HeartBeatContext(long time) {
            this.time = time;
        }

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }

        public int getAutoRead() {
            return autoRead;
        }

        public void setAutoRead(int autoRead) {
            this.autoRead = autoRead;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof HeartBeatContext)) return false;
            HeartBeatContext that = (HeartBeatContext) o;
            return time == that.time &&
                    autoRead == that.autoRead;
        }

        @Override
        public int hashCode() {

            return Objects.hash(time, autoRead);
        }

        @Override
        public String toString() {
            return "HeartBeatContext{" +
                    "time=" + time +
                    ", autoRead=" + autoRead +
                    '}';
        }
    }
}