package com.ctrip.framework.drc.fetcher.activity.replicator.handler.command;

import com.ctrip.framework.drc.core.driver.binlog.LogEventCallBack;
import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.converter.ByteBufConverter;
import com.ctrip.framework.drc.core.driver.command.handler.DrcBinlogDumpGtidCommandHandler;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.google.common.collect.Maps;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;

import java.util.Map;

/**
 * Created by mingdongli
 * 2019/9/24 上午11:06.
 */
public class FetcherBinlogDumpGtidCommandHandler extends DrcBinlogDumpGtidCommandHandler {

    private Map<Channel, LogEventCallBack> logEventCallBackMap = Maps.newConcurrentMap();

    public FetcherBinlogDumpGtidCommandHandler(LogEventHandler handler, ByteBufConverter converter) {
        super(handler, converter);
    }

    @Override
    protected LogEventCallBack getLogEventCallBack(Channel channel) {
        LogEventCallBack logEventCallBack = logEventCallBackMap.get(channel);
        if (logEventCallBack == null) {
            logEventCallBack = new LogEventCallBack() {
                @Override
                public void onSuccess() {
                    toggleAutoRead(channel, true);
                }

                @Override
                public void onFailure() {
                    toggleAutoRead(channel, false);
                }
            };
            logger.info("[LogEventCallBack] put for key {}:{}", channel, channel.hashCode());
            logEventCallBackMap.put(channel, logEventCallBack);
        }
        return logEventCallBack;
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