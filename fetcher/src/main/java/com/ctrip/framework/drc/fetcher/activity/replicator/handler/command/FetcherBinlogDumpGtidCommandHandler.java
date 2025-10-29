package com.ctrip.framework.drc.fetcher.activity.replicator.handler.command;

import com.ctrip.framework.drc.core.driver.binlog.LogEventCallBack;
import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.converter.ByteBufConverter;
import com.ctrip.framework.drc.core.driver.command.handler.DrcBinlogDumpGtidCommandHandler;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.xpipe.utils.MapUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;

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
                () -> {
                    addCloseListener(channel);
                    return new LogEventCallBack() {
                        private final Object flag = new Object();

                        @Override
                        public void onSuccess() {
                            synchronized (flag) {
                                toggleAutoRead(channel, true);
                                dispose();
                            }
                            onHeartBeat();
                        }

                        @Override
                        public void onFailure() {
                            synchronized (flag) {
                                toggleAutoRead(channel, false);
                            }
                        }

                        @Override
                        public Channel getChannel() {
                            return channel;
                        }

                        @Override
                        public void dispose() {
                        }
                    };
                }
        );
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