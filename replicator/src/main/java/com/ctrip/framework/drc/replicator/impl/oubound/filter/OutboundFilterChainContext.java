package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.row.RowsFilterContext;
import io.netty.channel.Channel;

import java.util.Map;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class OutboundFilterChainContext {

    private Channel channel;

    private ConsumeType consumeType;

    private Map<Long, TableMapLogEvent> tableMapWithinTransaction;

    private RowsFilterContext filterContext;

    public OutboundFilterChainContext(Channel channel, ConsumeType consumeType, Map<Long, TableMapLogEvent> tableMapWithinTransaction, RowsFilterContext filterContext) {
        this.channel = channel;
        this.consumeType = consumeType;
        this.tableMapWithinTransaction = tableMapWithinTransaction;
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

    public Map<Long, TableMapLogEvent> getTableMapWithinTransaction() {
        return tableMapWithinTransaction;
    }
}
