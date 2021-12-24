package com.ctrip.framework.drc.core.driver.command.impl.replicator;

import com.ctrip.framework.drc.core.driver.binlog.EventStatusAware;
import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.driver.command.packet.client.BinlogDumpGtidCommandPacket;
import com.ctrip.framework.drc.core.driver.command.packet.server.EOFPacket;
import com.ctrip.framework.drc.core.driver.command.packet.server.ErrorPacket;
import com.ctrip.framework.drc.core.driver.util.MySQLConstants;
import com.ctrip.framework.drc.core.server.observer.event.LogEventObservable;
import com.ctrip.framework.drc.core.server.observer.event.LogEventObserver;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 * <p>
 * Sep 01, 2019
 */
public class ComBinlogDumpGtidCommand extends AbstractMySQLCommand<ResultCode> implements LogEventObservable, EventStatusAware {

    protected List<Observer> observers = Lists.newCopyOnWriteArrayList();

    private boolean halfEvent = false;

    public ComBinlogDumpGtidCommand(final BinlogDumpGtidCommandPacket packet, final SimpleObjectPool<NettyClient> clientPool, final ScheduledExecutorService scheduled) {
        super(packet, clientPool, scheduled);
    }

    @Override
    protected ResultCode doReceiveResponse(Channel channel, ByteBuf byteBuf) {
        try {
            if (!halfEvent) {
                if (byteBuf.getByte(MySQLConstants.HEADER_PACKET_LENGTH) < 0) {
                    if ((0xff & byteBuf.getByte(MySQLConstants.HEADER_PACKET_LENGTH)) == 0xfe) {
                        EOFPacket eofPacket = new EOFPacket();
                        eofPacket.read(byteBuf);
                        return ResultCode.HANDLE_SUCCESS;
                    } else {
                        ErrorPacket errorPacket = new ErrorPacket();
                        errorPacket.read(byteBuf);
                        String errorMessage = "dump command error : " + errorPacket.message;
                        getLogger().error(errorMessage);
                        throw new RuntimeException(errorMessage);
                    }
                }
                byteBuf.skipBytes(MySQLConstants.HEADER_PACKET_LENGTH + 1); //4 header + 1 mark
            } else {
                byteBuf.skipBytes(MySQLConstants.HEADER_PACKET_LENGTH); //4 header
            }

            for (Observer observer : observers) {
                if (observer instanceof LogEventObserver) {
                    observer.update(Pair.from(byteBuf, channel), this);
                }
            }
        } catch (Exception e) {
            getLogger().error("doReceiveResponse error", e);
            throw e;
        }
        return null;
    }

    @Override
    public SERVER_COMMAND getCommandType() {
        return SERVER_COMMAND.COM_BINLOG_DUMP_GTID;
    }

    @Override
    public void addObserver(Observer observer) {
        if (!observers.contains(observer)) {
            observers.add(observer);
        }
    }

    @Override
    protected boolean logRequest() {
        return false;
    }

    @Override
    public void removeObserver(Observer observer) {
        observers.remove(observer);
    }

    @Override
    public void setHalfEvent(boolean halfEvent) {
        this.halfEvent = halfEvent;
    }
}
