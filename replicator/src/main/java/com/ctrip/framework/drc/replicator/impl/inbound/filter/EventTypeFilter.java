package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.google.common.collect.Sets;

import java.util.HashSet;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.*;
import static com.ctrip.framework.drc.replicator.impl.inbound.filter.TransactionFlags.OTHER_F;

/**
 * Created by mingdongli
 * 2019/10/9 上午10:08.
 */
public class EventTypeFilter extends AbstractLogEventFilter<InboundLogEventContext> {

    private HashSet<LogEventType> SKIP_EVENT_TYPE = Sets.newHashSet(rotate_log_event, previous_gtids_log_event, format_description_log_event, heartbeat_log_event);

    private HashSet<LogEventType> DRC_HEARTBEAT_EVENT_TYPE = Sets.newHashSet(drc_heartbeat_log_event);

    private HashSet<LogEventType> NOT_SKIP_EVENT_TYPE = Sets.newHashSet(drc_ddl_log_event);

    @Override
    public boolean doFilter(InboundLogEventContext value) {

        LogEventType logEventType = value.getLogEvent().getLogEventType();

        boolean skip = SKIP_EVENT_TYPE.contains(logEventType);

        if (DRC_HEARTBEAT_EVENT_TYPE.contains(logEventType)) {
            value.getCallBack().onHeartHeat();
            skip = true;
        } else if (NOT_SKIP_EVENT_TYPE.contains(logEventType)) {  //when receive drc_ddl_log_event, not set inExcludeGroup, or will exclude gtid event.
            value.reset();
            skip = false;
        }

        if (skip) {
            value.reset();
            value.mark(OTHER_F);
        }

        return doNext(value, skip);
    }

}
