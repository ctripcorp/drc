package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.UpdateRowsEvent;
import com.ctrip.framework.drc.core.server.common.filter.AbstractPostLogEventFilter;
import com.ctrip.framework.drc.replicator.impl.monitor.DefaultMonitorManager;
import org.apache.commons.lang3.StringUtils;

/**
 * post filter
 * @Author limingdong
 * @create 2020/4/24
 */
public class DelayMonitorFilter extends AbstractPostLogEventFilter<LogEventInboundContext> {

    private DefaultMonitorManager delayMonitor;

    private String previousGtid = StringUtils.EMPTY;

    public DelayMonitorFilter(DefaultMonitorManager delayMonitor) {
        this.delayMonitor = delayMonitor;
    }

    @Override
    public boolean doFilter(LogEventInboundContext value) {

        boolean filtered = doNext(value, value.isInExcludeGroup());

        LogEvent logEvent = value.getLogEvent();
        LogEventType logEventType = logEvent.getLogEventType();

        if (filtered) {
            switch (logEventType) {
                case gtid_log_event:
                    previousGtid = ((GtidLogEvent) logEvent).getGtid();
                    break;
                case table_map_log_event:
                    TableMapLogEvent tableMapLogEvent = (TableMapLogEvent) logEvent;
                    delayMonitor.onTableMapLogEvent(tableMapLogEvent);
                    break;
                case update_rows_event_v2:
                    UpdateRowsEvent updateRowsEvent = (UpdateRowsEvent) logEvent;
                    if(delayMonitor.onUpdateRowsEvent(updateRowsEvent, previousGtid)){
                        previousGtid = StringUtils.EMPTY;
                    }
                    break;
                default:
                    break;
            }
        }

        return filtered;
    }


}
