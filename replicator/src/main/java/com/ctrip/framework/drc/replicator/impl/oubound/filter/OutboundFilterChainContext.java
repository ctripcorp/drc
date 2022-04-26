package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.enums.RowFilterType;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterContext;
import io.netty.channel.Channel;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class OutboundFilterChainContext {

    private Channel channel;

    private ConsumeType consumeType;

    private RowsFilterContext filterContext;

    public OutboundFilterChainContext(Channel channel, ConsumeType consumeType, RowsFilterContext filterContext) {
        this.channel = channel;
        this.consumeType = consumeType;
        this.filterContext = filterContext;
    }

    public RowsFilterContext getFilterContext() {
        return filterContext;
    }

    public Channel getChannel() {
        return channel;
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public RowFilterType getRowFilterType() {
        return filterContext.getFilterType();
    }

    public static OutboundFilterChainContext from(Channel channel, ConsumeType consumeType, RowsFilterContext filterContext) {
        return new OutboundFilterChainContext(channel, consumeType, filterContext);
    }

}
