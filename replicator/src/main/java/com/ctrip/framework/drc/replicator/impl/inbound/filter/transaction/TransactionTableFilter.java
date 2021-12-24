package com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.ITransactionEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.UpdateRowsEvent;
import com.ctrip.framework.drc.replicator.impl.monitor.MonitorManager;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_TRANSACTION_TABLE_NAME;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.EVENT_LOGGER;

/**
 * Created by jixinwang on 2021/10/9
 */
public class TransactionTableFilter extends AbstractTransactionFilter {

    private MonitorManager delayMonitor;

    private String previousGtid = StringUtils.EMPTY;

    public TransactionTableFilter(MonitorManager delayMonitor) {
        this.delayMonitor = delayMonitor;
    }

    @Override
    public boolean doFilter(ITransactionEvent transactionEvent) {
        boolean isTransactionTable = false;

        List<LogEvent> logEvents = transactionEvent.getEvents();
        for (LogEvent logEvent : logEvents) {
            LogEventType logEventType = logEvent.getLogEventType();
            switch (logEventType) {
                case gtid_log_event:
                    previousGtid = ((GtidLogEvent) logEvent).getGtid();
                    break;
                case table_map_log_event:
                    String tableName = ((TableMapLogEvent) logEvent).getTableName();
                    if (DRC_TRANSACTION_TABLE_NAME.equalsIgnoreCase(tableName)) {
                        isTransactionTable = true;
                    }
                    if (isTransactionTable) {
                        delayMonitor.onTableMapLogEvent((TableMapLogEvent) logEvent);
                    }
                    break;
                case update_rows_event_v2:
                    if (isTransactionTable) {
                        if(delayMonitor.onUpdateRowsEvent((UpdateRowsEvent) logEvent, previousGtid)){
                            previousGtid = StringUtils.EMPTY;
                        }
                    }
                    break;
                default:
                    break;
            }
        }

        if (isTransactionTable) {
            convertTransactionTable(transactionEvent);
        } else {
            doNext(transactionEvent, false);
        }
        return isTransactionTable;
    }

    private void convertTransactionTable(ITransactionEvent transactionEvent) {
        List<LogEvent> logEvents = transactionEvent.getEvents();
        GtidLogEvent gtidLogEvent = (GtidLogEvent) logEvents.get(0);
        gtidLogEvent.setEventType(LogEventType.drc_gtid_log_event.getType());
        transactionEvent.setEvents(Lists.newArrayList(gtidLogEvent));
        gtidLogEvent.setNextTransactionOffsetAndUpdateEventSize(0);
        releaseRedundantEvents(logEvents);
    }

    private void releaseRedundantEvents(List<LogEvent> logEvents) {
        for (int i = 1; i < logEvents.size(); i++) {
            LogEvent logEvent = logEvents.get(i);
            LogEventType logEventType = logEvent.getLogEventType();
            try {
                logEvent.release();
            } catch (Exception e) {
                EVENT_LOGGER.error("released logEventType of {} error when release redundant events", logEventType, e);
            }
        }
    }
}
