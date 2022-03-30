package com.ctrip.framework.drc.fetcher.activity.replicator.handler.command;

import com.ctrip.framework.drc.core.driver.binlog.LogEventCallBack;
import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.converter.ByteBufConverter;
import com.ctrip.framework.drc.core.driver.command.handler.DrcBinlogDumpGtidCommandHandler;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.utils.MapUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.MASTER_HEARTBEAT_PERIOD_SECONDS;

/**
 * for applier dump binlog from replicator
 * Created by mingdongli
 * 2019/9/24 上午11:06.
 */
public class FetcherBinlogDumpGtidCommandHandler extends DrcBinlogDumpGtidCommandHandler {

    public FetcherBinlogDumpGtidCommandHandler(LogEventHandler handler, ByteBufConverter converter) {
        super(handler, converter);
    }

    @Override
    protected LogEventCallBack getLogEventCallBack(Channel channel) {
        return MapUtils.getOrCreate(logEventCallBackMap, channel,
                () -> new LogEventCallBack() {
                    private ScheduledExecutorService scheduledExecutorService;
                    private ScheduledFuture future;

                    @Override
                    public void onSuccess() {
                        toggleAutoRead(channel, true);
                        if (future != null) {
                            future.cancel(false);
                        }
                        if (scheduledExecutorService != null) {
                            scheduledExecutorService.shutdownNow();
                            scheduledExecutorService = null;
                        }
                        onHeartHeat();
                    }

                    @Override
                    public void onFailure() {
                        toggleAutoRead(channel, false);
                        if (scheduledExecutorService == null) {
                            scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("AutoRead");
                        }
                        future = scheduledExecutorService.scheduleAtFixedRate(() -> onHeartHeat(), 0, MASTER_HEARTBEAT_PERIOD_SECONDS, TimeUnit.SECONDS);
                    }

                    @Override
                    public Channel getChannel() {
                        return channel;
                    }
                });
    }

    private synchronized void toggleAutoRead(Channel channel, boolean autoRead) {
        try {
            ChannelConfig channelConfig = channel.config();
            if (channelConfig.isAutoRead() != autoRead) {
                channelConfig.setAutoRead(autoRead);
                if (autoRead) {
                    channel.read();
                }
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.applier.autoread", channel + ":" + String.valueOf(autoRead));
                logger.info("[AutoRead] set to {} for {}:{}", autoRead, channel, channel.hashCode());
            } else {
                logger.warn("[AutoRead] ignore set to {} for {}:{}", autoRead, channel, channel.hashCode());
            }
        } catch (Exception e) {
            logger.error("[AutoRead] set to {} for {} error", autoRead, channel, e);
        }
    }
}