package com.ctrip.framework.drc.core.driver.command.impl.replicator;

import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.driver.command.packet.client.BinlogDumpGtidCommandPacket;
import com.ctrip.framework.drc.core.server.observer.event.LogEventObserver;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @Author limingdong
 * @create 2020/5/11
 */
public class ComBackupBinlogDumpGtidCommand extends ComBinlogDumpGtidCommand {

    public ComBackupBinlogDumpGtidCommand(BinlogDumpGtidCommandPacket packet, SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
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
}
