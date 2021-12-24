package com.ctrip.framework.drc.core.driver.command.impl.applier;

import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.command.impl.replicator.AbstractMySQLCommand;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
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
 * Created by mingdongli
 * 2019/9/24 上午11:04.
 */
public class ComApplierBinlogDumpGtidCommand extends AbstractMySQLCommand<ResultCode> implements LogEventObservable {

    private List<Observer> observers = Lists.newCopyOnWriteArrayList();

    public ComApplierBinlogDumpGtidCommand(final ApplierDumpCommandPacket packet, final SimpleObjectPool<NettyClient> clientPool, final ScheduledExecutorService scheduled) {
        super(packet, clientPool, scheduled);
    }

    @Override
    protected ResultCode doReceiveResponse(Channel channel, ByteBuf byteBuf) {
        for (Observer observer : observers) {
            if (observer instanceof LogEventObserver) {
                observer.update(Pair.from(byteBuf, channel), this);
            }
        }
        return null;
    }

    /**
     * 如果不覆写，会重新创建连接池
     */
    protected void doReset() {
        getLogger().info("[doReset] do nothing in {}", getClass().getSimpleName());
        //do nothing
    }

    @Override
    public SERVER_COMMAND getCommandType() {
        return SERVER_COMMAND.COM_APPLIER_BINLOG_DUMP_GTID;
    }

    @Override
    public void addObserver(Observer observer) {
        if (!observers.contains(observer)) {
            observers.add(observer);
        }
    }

    @Override
    public void removeObserver(Observer observer) {
        observers.remove(observer);
    }
}
