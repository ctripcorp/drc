package com.ctrip.framework.drc.replicator.impl.oubound.handler;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.DelayMonitorLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.ReferenceCountedDelayMonitorLogEvent;
import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;
import com.ctrip.framework.drc.core.driver.command.packet.monitor.DelayMonitorCommandPacket;
import com.ctrip.framework.drc.core.monitor.column.DelayMonitorColumn;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.replicator.impl.inbound.event.ObservableLogEventHandler;
import com.ctrip.framework.drc.replicator.impl.oubound.observer.MonitorEventObservable;
import com.ctrip.framework.drc.replicator.impl.oubound.observer.MonitorEventObserver;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.utils.Gate;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND.COM_DELAY_MONITOR;

/**
 * Created by mingdongli
 * 2019/12/12 下午7:33.
 */
public class DelayMonitorCommandHandler extends AbstractServerCommandHandler implements CommandHandler {

    private ExecutorService dumpExecutorService;

    private ConcurrentMap<DelayMonitorKey, NettyClient> delayMonitorClient = Maps.newConcurrentMap();

    private ObservableLogEventHandler logEventHandler;

    private String registryKey;

    public DelayMonitorCommandHandler(ObservableLogEventHandler logEventHandler, String registryKey) {
        this.logEventHandler = logEventHandler;
        this.registryKey = registryKey;
    }

    @Override
    public synchronized void handle(ServerCommandPacket serverCommandPacket, NettyClient nettyClient) {
        DelayMonitorCommandPacket monitorCommandPacket = (DelayMonitorCommandPacket) serverCommandPacket;
        Channel channel = nettyClient.channel();
        logger.info("[Receive] command code is {} for {}", COM_DELAY_MONITOR.name(), channel);
        String dcName = monitorCommandPacket.getDcName();
        String clusterName = monitorCommandPacket.getClusterName();
        InetSocketAddress remoteAddress = (InetSocketAddress) channel.remoteAddress();
        String ip = remoteAddress.getAddress().getHostAddress();
        DelayMonitorKey key = getKey(monitorCommandPacket, ip);
        if (!delayMonitorClient.containsKey(key)) {
            delayMonitorClient.putIfAbsent(key, nettyClient);
            MonitorEventTask delayMonitorEventTask = new MonitorEventTask(nettyClient.channel(), monitorCommandPacket, key);
            logEventHandler.addObserver(delayMonitorEventTask);
            dumpExecutorService.submit(delayMonitorEventTask);
            logger.info("[Receive] request for {} delay monitor {}:{}", registryKey, dcName, clusterName);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.console.dump", key.toString());
        } else {
            logger.info("[Duplicate] request for {} delay monitor {}:{} and close channel {}", registryKey, dcName, clusterName, channel);
            channel.close();
        }
    }

    private DelayMonitorKey getKey(DelayMonitorCommandPacket monitorCommandPacket, String ip) {
        String dcName = monitorCommandPacket.getDcName();
        String clusterName = monitorCommandPacket.getClusterName();
        return new DelayMonitorKey(dcName, clusterName, ip);
    }

    @Override
    public void initialize() {
        this.dumpExecutorService = ThreadUtils.newCachedThreadPool("Delay-DefaultMonitorManager-" + registryKey);
    }

    @Override
    public void dispose() {
        for (Map.Entry<DelayMonitorKey, NettyClient> consoleKey : delayMonitorClient.entrySet()) {
            try {
                consoleKey.getValue().channel().close();
                logger.info("[NettyClient] close for {} in DelayMonitorCommandHandler", consoleKey.getKey().toString());
            } catch (Exception e) {
                logger.error("consoleKey close NettyClient error", e);
            }
        }
        dumpExecutorService.shutdown();
    }

    @Override
    public SERVER_COMMAND getCommandType() {
        return COM_DELAY_MONITOR;
    }

    public class MonitorEventTask implements Runnable, MonitorEventObserver {

        private Logger logger = LoggerFactory.getLogger(getClass());

        private Gate gate;

        private Channel channel;

        private DelayMonitorCommandPacket monitorCommandPacket;

        private ArrayBlockingQueue<LogEvent> delayBlockingQueue = new ArrayBlockingQueue<>(10);

        private volatile boolean channelClosed = false;

        private DelayMonitorKey key;

        public MonitorEventTask(Channel channel, DelayMonitorCommandPacket monitorCommandPacket, DelayMonitorKey key) {
            this.channel = channel;
            this.monitorCommandPacket = monitorCommandPacket;
            this.key = key;
            this.gate = channel.attr(ReplicatorMasterHandler.KEY_CLIENT).get().getGate();
        }


        @Override
        public void run() {
            try {
                this.channel.closeFuture().addListener((ChannelFutureListener) future -> {
                    Throwable throwable = future.cause();
                    if (throwable != null) {
                        logger.error("MonitorEventTask closeFuture", throwable);
                    }
                    channelClosed = true;
                    gate.open();
                    logger.info("MonitorEventTask closeFuture Listener invoke open gate {} and set channelClosed", gate);
                    clearResource();
                    logger.info("[Remove] MonitorEventObserver from delayMonitorEventObservable");
                });
                while (!channelClosed) {
                    LogEvent logEvent = delayBlockingQueue.poll(4, TimeUnit.SECONDS);
                    if (logEvent == null) {
                        continue;
                    }
                    gate.tryPass();
                    if (channelClosed) {
                        logger.info("channelClosed and return MonitorEventTask");
                        return;
                    }
                    logEvent.write(new IoCache() {
                        @Override
                        public void write(byte[] data) {

                        }

                        @Override
                        public void write(Collection<ByteBuf> byteBufs) {
                            for (ByteBuf b : byteBufs) {
                                b.readerIndex(0);
                                channel.write(b);
                            }
                            channel.flush();
                        }

                        @Override
                        public void write(Collection<ByteBuf> byteBuf, boolean isDdl) {
                        }

                        @Override
                        public void write(LogEvent logEvent) {

                        }
                    });
                    DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.delay.consume", key.toString());
                }
            } catch (Throwable e) {
                logger.error("delay monitor thread error", e);
            }
        }

        @Override
        public void update(Object args, Observable observable) {
            if (observable instanceof MonitorEventObservable && args instanceof LogEvent) {
                LogEvent logEvent = (LogEvent) args;

                if (logEvent instanceof ReferenceCountedDelayMonitorLogEvent) {
                    ReferenceCountedDelayMonitorLogEvent delayMonitorLogEvent = (ReferenceCountedDelayMonitorLogEvent) logEvent;
                    String delayMonitorSrcDcName = delayMonitorLogEvent.getSrcDcName();
                    if (delayMonitorSrcDcName == null) {
                        delayMonitorSrcDcName = DelayMonitorColumn.getDelayMonitorSrcDcName(delayMonitorLogEvent);
                    }
                    if (!key.srcDcName.equalsIgnoreCase(delayMonitorSrcDcName)) {
                        ((ReferenceCountedDelayMonitorLogEvent) logEvent).release(1);
                        return;
                    }
                }

                boolean added = delayBlockingQueue.offer(logEvent);
                if (!added) {
                    release(logEvent);
                }
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.delay.produce", key.toString());
                if (logger.isDebugEnabled()) {
                    logger.debug("[Offer] LogEvent to delayBlockingQueue with result {}", added);
                }
            }
        }

        private void release(LogEvent logEvent) {
            try {
                if (logEvent instanceof DelayMonitorLogEvent) {
                    ((DelayMonitorLogEvent) logEvent).release();
                } else {
                    logEvent.release();
                }
            } catch (Exception e) {
                logger.error("[Release] logEvent error", e);
            }
        }

        private void clearResource() {
            logEventHandler.removeObserver(this);
            while (!delayBlockingQueue.isEmpty()) {
                LogEvent logEvent = delayBlockingQueue.poll();
                try {
                    logEvent.release();
                } catch (Exception e) {
                    logger.error("[Release] logEvent error", e);
                }
            }
            NettyClient nettyClient = delayMonitorClient.remove(key);
            nettyClient.channel().close();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MonitorEventTask that = (MonitorEventTask) o;
            return Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {

            return Objects.hash(channel, monitorCommandPacket);
        }
    }

    private static class DelayMonitorKey {

        private String srcDcName;

        private String clusterName;

        private String ip;

        public DelayMonitorKey(String srcDcName, String clusterName, String ip) {
            this.srcDcName = srcDcName;
            this.clusterName = clusterName;
            this.ip = ip;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof DelayMonitorKey)) return false;
            DelayMonitorKey that = (DelayMonitorKey) o;
            return Objects.equals(srcDcName, that.srcDcName) &&
                    Objects.equals(clusterName, that.clusterName) &&
                    Objects.equals(ip, that.ip);
        }

        @Override
        public int hashCode() {

            return Objects.hash(srcDcName, clusterName, ip);
        }

        @Override
        public String toString() {
            return "DelayMonitorKey{" +
                    "srcDcName='" + srcDcName + '\'' +
                    ", clusterName='" + clusterName + '\'' +
                    ", ip='" + ip + '\'' +
                    '}';
        }
    }
}
