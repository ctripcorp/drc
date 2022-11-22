package com.ctrip.framework.drc.core.driver.command.handler;

import com.ctrip.framework.drc.core.driver.binlog.EventStatusAware;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.LogEventCallBack;
import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.converter.ByteBufConverter;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcErrorLogEvent;
import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.framework.drc.core.driver.command.impl.replicator.ComBinlogDumpGtidCommand;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.driver.command.packet.client.BinlogDumpGtidCommandPacket;
import com.ctrip.framework.drc.core.exception.LogEventException;
import com.ctrip.framework.drc.core.exception.dump.EventConvertException;
import com.ctrip.framework.drc.core.server.observer.event.LogEventObservable;
import com.ctrip.framework.drc.core.server.observer.event.LogEventObserver;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;

import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.drc_error_log_event;

/**
 * for replicator master dump binlog from mysql
 * Created by mingdongli
 * 2019/9/18 下午7:44.
 */
public class BinlogDumpGtidClientCommandHandler extends AbstractClientCommandHandler<ResultCode> implements CommandHandler<ResultCode>, LogEventObserver {

    protected ByteBufConverter converter;

    protected LogEventHandler handler;

    protected Map<Channel, LogEventCallBack> logEventCallBackMap = Maps.newConcurrentMap();

    public BinlogDumpGtidClientCommandHandler(LogEventHandler handler, ByteBufConverter converter) {
        this.converter = converter;
        this.handler = handler;
    }

    @Override
    public CommandFuture<ResultCode> handle(ServerCommandPacket serverCommandPacket, SimpleObjectPool<NettyClient> simpleObjectPool) {
        if (serverCommandPacket instanceof BinlogDumpGtidCommandPacket) {
            BinlogDumpGtidCommandPacket binlogDumpGtidCommandPacket = (BinlogDumpGtidCommandPacket) serverCommandPacket;
            ComBinlogDumpGtidCommand dumpGtidCommand = new ComBinlogDumpGtidCommand(binlogDumpGtidCommandPacket, simpleObjectPool, scheduledExecutorService);
            dumpGtidCommand.addObserver(this);
            return execute(dumpGtidCommand);
        }
        return new DefaultCommandFuture<>();
    }

    @Override
    public void update(Object args, Observable observable) {
        if (observable instanceof LogEventObservable) {
            Pair<ByteBuf, Channel> message = (Pair<ByteBuf, Channel>) args;
            ByteBuf byteBuf = message.getKey();
            List<LogEvent> logEvents = null;
            try {
                logEvents = converter.convert(byteBuf);
            } catch (Throwable t) {
                setHalfEvent(observable, false);
                handler.onLogEvent(null, null, new EventConvertException("converter.convert() - UNLIKELY", t));
            }
            if (null != logEvents && !logEvents.isEmpty()) {
                setHalfEvent(observable, false);
                for (LogEvent event : logEvents) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[REPLICATOR] receive {}", event.getLogEventType());
                    }
                    if (drc_error_log_event == event.getLogEventType()) {
                        DrcErrorLogEvent errorLogEvent = (DrcErrorLogEvent) event;
                        ResultCode resultCode = ResultCode.getResultCode(errorLogEvent.getErrorNumber());
                        ErrorMessage<ResultCode> errorMessage = new ErrorMessage<>(resultCode, resultCode.getMessage());
                        errorLogEvent.release();
                        throw new LogEventException(errorMessage, null);
                    }
                }
                LogEventHandler logEventHandler = handler;
                for (LogEvent logEvent : logEvents) {
                    logEventHandler.onLogEvent(logEvent, getLogEventCallBack(message.getValue()), null);
                }
            } else {
                setHalfEvent(observable, true);
            }
        }

    }

    private void setHalfEvent(Observable observable, boolean halfEvent) {
        if (observable instanceof EventStatusAware) {
            ((EventStatusAware) observable).setHalfEvent(halfEvent);
        }
    }

    protected LogEventCallBack getLogEventCallBack(Channel channel) {
        return MapUtils.getOrCreate(logEventCallBackMap, channel,
                () -> {
                    addCloseListener(channel);
                    return () -> channel;
                });
    }

    protected void addCloseListener(Channel channel) {
        channel.closeFuture().addListener((ChannelFutureListener) future -> {
            LogEventCallBack logEventCallBack = logEventCallBackMap.remove(channel);
            logEventCallBack.dispose();
            logger.info("[Remove] {}:{} from logEventCallBackMap and dispose", channel, logEventCallBack);
        });
    }

}
