package com.ctrip.framework.drc.core.driver.command.impl.delay;

import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.command.impl.replicator.AbstractMySQLCommand;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.driver.command.packet.monitor.DelayMonitorCommandPacket;
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
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-02
 */
public class DelayMonitorCommand extends AbstractMySQLCommand<ResultCode> implements LogEventObservable {

    private List<Observer> observers = Lists.newCopyOnWriteArrayList();

    public DelayMonitorCommand(final DelayMonitorCommandPacket packet, final SimpleObjectPool<NettyClient> clientPool, final ScheduledExecutorService scheduled) {
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
     * reference ComApplierBinlogDumpGtidCommand as the connection pool will be recreated if not rewritted
     */
    @Override
    protected void doReset() {
        getLogger().info("[doReset] do nothing in {}", getClass().getSimpleName());
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

    @Override
    public SERVER_COMMAND getCommandType() {
        return SERVER_COMMAND.COM_DELAY_MONITOR;
    }

    @Override
    public String toString() {
        return "DelayMonitorCommand{" +
                "observers=" + observers +
                ", scheduled=" + scheduled +
                '}';
    }
}
