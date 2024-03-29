package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TransactionTableMarkedTableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.UpdateRowsEvent;
import com.ctrip.framework.drc.core.server.common.filter.AbstractPostLogEventFilter;
import com.ctrip.framework.drc.replicator.impl.monitor.DefaultMonitorManager;
import org.apache.commons.lang3.StringUtils;

/**
 * post filter
 * @Author limingdong
 * @create 2020/4/24
 */
public class DelayMonitorFilter extends AbstractPostLogEventFilter<InboundLogEventContext> {

    private DefaultMonitorManager delayMonitor;

    private String previousGtid = StringUtils.EMPTY;

    public DelayMonitorFilter(DefaultMonitorManager delayMonitor) {
        this.delayMonitor = delayMonitor;
    }

    @Override
    public boolean doFilter(InboundLogEventContext value) {

        boolean filtered = doNext(value, value.isInExcludeGroup());

        LogEvent logEvent = value.getLogEvent();
        LogEventType logEventType = logEvent.getLogEventType();

        if (shouldProcess(filtered)) {
            switch (logEventType) {
                case gtid_log_event:
                    previousGtid = ((GtidLogEvent) logEvent).getGtid();
                    break;
                case table_map_log_event:
                    TableMapLogEvent tableMapLogEvent = logEvent instanceof TableMapLogEvent
                            ? (TableMapLogEvent) logEvent
                            : ((TransactionTableMarkedTableMapLogEvent) logEvent).getDelegate();
                    delayMonitor.onTableMapLogEvent(tableMapLogEvent);
                    break;
                case update_rows_event_v2:
                    UpdateRowsEvent updateRowsEvent = (UpdateRowsEvent) logEvent;
                    delayMonitor.onUpdateRowsEvent(updateRowsEvent, previousGtid);
                    break;
                default:
                    break;
            }
        }

        return filtered;
    }

    @Override
    public void reset() {
        previousGtid = StringUtils.EMPTY;
        super.reset();
    }

    protected boolean shouldProcess(boolean filtered) {
        return filtered;
    }

}
