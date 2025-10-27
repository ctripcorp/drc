package com.ctrip.framework.drc.fetcher.activity.replicator.handler;

import com.ctrip.framework.drc.core.driver.command.netty.codec.*;
import com.ctrip.framework.drc.fetcher.activity.replicator.handler.command.FetcherCommandResultHandler;
import io.netty.channel.ChannelHandler;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.ArrayList;
import java.util.List;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.APPLIER_HEARTBEAT_PERIOD_SECONDS_WRITEIDLE;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONNECTION_IDLE_TIMEOUT_SECOND;

/**
 * Created by mingdongli
 * 2019/9/23 下午9:03.
 */
public class FetcherChannelHandlerFactory extends DrcChannelHandlerFactory implements ChannelHandlerFactory {
    @Override
    public List<ChannelHandler> createChannelHandlers() {
        List<ChannelHandler> handlerList = new ArrayList<>();
        handlerList.add(new IdleStateHandler(CONNECTION_IDLE_TIMEOUT_SECOND, APPLIER_HEARTBEAT_PERIOD_SECONDS_WRITEIDLE, 0));
        handlerList.add(new PackageEncoder());
        handlerList.add(new FileEventDecode());
        handlerList.add(new FetcherCommandResultHandler());
        return handlerList;
    }
}
