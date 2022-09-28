package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.LogEventCallBack;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.server.common.filter.Resettable;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.EVENT_LOGGER;

/**
 * Created by mingdongli
 * 2019/10/9 上午10:27.
 */
public class InboundLogEventContext implements Resettable {

    private LogEvent logEvent;

    private LogEventCallBack callBack;

    private TransactionFlags flags;

    private String gtid;

    private boolean notRelease = false;

    public InboundLogEventContext(LogEvent logEvent, LogEventCallBack callBack, TransactionFlags flags, String gtid) {
        this.logEvent = logEvent;
        this.callBack = callBack;
        this.flags = flags;
        this.gtid = gtid;
    }

    public LogEvent getLogEvent() {
        return logEvent;
    }

    public void setLogEvent(LogEvent logEvent) {
        this.logEvent = logEvent;
    }

    public boolean isInExcludeGroup() {
        return flags.filtered();
    }

    public void mark(int flag) {
        flags.mark(flag);
    }

    public void unmark(int flag) {
        flags.unmark(flag);
    }

    public boolean isBlackTableFiltered() {
        return flags.blackTableFiltered();
    }

    public boolean isTransactionTableRelated() {
        return flags.transactionTableFiltered();
    }

    public boolean isGtidFiltered() {
        return flags.gtidFiltered();
    }

    public boolean isOtherFiltered() {
        return flags.otherFiltered();
    }

    public String getGtid() {
        return gtid;
    }

    public void setGtid(String gtid) {
        this.gtid = gtid;
    }

    public LogEventCallBack getCallBack() {
        return callBack;
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
            if (logEventType == LogEventType.xid_log_event || isOtherFiltered() || (LogEventUtils.isDrcEvent(logEventType) && !LogEventUtils.isDrcGtidLogEvent(logEventType))) {
                reset();
            }
        }
    }

    public void setNotRelease(boolean notRelease) {
        this.notRelease = notRelease;
    }

    @Override
    public void reset() {
        flags.reset();
    }
}
