package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.EVENT_LOGGER;

/**
 * Created by mingdongli
 * 2019/10/9 上午10:27.
 */
public class LogEventWithGroupFlag {

    private LogEvent logEvent;

    private boolean inExcludeGroup;

    private boolean tableFiltered;

    private boolean transactionTableRelated;

    private String gtid;

    private boolean notRelease = false;

    public LogEventWithGroupFlag(LogEvent logEvent, boolean inExcludeGroup, boolean tableFiltered, boolean transactionTableRelated, String gtid) {
        this.logEvent = logEvent;
        this.inExcludeGroup = inExcludeGroup;
        this.tableFiltered = tableFiltered;
        this.transactionTableRelated = transactionTableRelated;
        this.gtid = gtid;
    }

    public LogEvent getLogEvent() {
        return logEvent;
    }

    public void setLogEvent(LogEvent logEvent) {
        this.logEvent = logEvent;
    }

    public boolean isInExcludeGroup() {
        return inExcludeGroup;
    }

    public void setInExcludeGroup(boolean inExcludeGroup) {
        this.inExcludeGroup = inExcludeGroup;
    }

    public boolean isTableFiltered() {
        return tableFiltered;
    }

    public void setTableFiltered(boolean tableFiltered) {
        this.tableFiltered = tableFiltered;
    }

    public boolean isTransactionTableRelated() {
        return transactionTableRelated;
    }

    public void setTransactionTableRelated(boolean transactionTableRelated) {
        this.transactionTableRelated = transactionTableRelated;
    }

    public String getGtid() {
        return gtid;
    }

    public void setGtid(String gtid) {
        this.gtid = gtid;
    }

    public void releaseEvent() {
        LogEventType logEventType = logEvent.getLogEventType();
        try {
            if (!notRelease) {
                logEvent.release();
            }
        } catch (Exception e) {
            EVENT_LOGGER.error("release {} of {} error", logEventType, gtid, e);
        } finally {
            setNotRelease(false);
            if (logEventType == LogEventType.xid_log_event || LogEventUtils.isDrcEvent(logEventType)) {
                setInExcludeGroup(false);
            }
        }
    }

    public void setNotRelease(boolean notRelease) {
        this.notRelease = notRelease;
    }
}
