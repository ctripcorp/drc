package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.XidLogEvent;
import com.ctrip.framework.drc.core.server.common.filter.AbstractPostLogEventFilter;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.TransactionCache;
import org.apache.commons.lang3.StringUtils;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.gtid_log_event;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.xid_log_event;
import static com.ctrip.framework.drc.replicator.impl.inbound.filter.TransactionFlags.BLACK_TABLE_NAME_F;

/**
 * post filter
 * @Author limingdong
 * @create 2020/4/24
 */
public class PersistPostFilter extends AbstractPostLogEventFilter<InboundLogEventContext> {

    public static final long FAKE_XID_PARAM = 100l;

    public static final long FAKE_SERVER_PARAM = 1l;

    private TransactionCache transactionCache;

    private boolean circularBreak = false;

    public PersistPostFilter(TransactionCache transactionCache) {
        this.transactionCache = transactionCache;
    }

    @Override
    public boolean doFilter(InboundLogEventContext value) {

        boolean filtered = doNext(value, value.isInExcludeGroup());  //post filter

        LogEvent logEvent = value.getLogEvent();
        LogEventType logEventType = logEvent.getLogEventType();

        if (filtered) {
            if (gtid_log_event == logEventType) {  //persist drc_gtid_log_event
                checkXid(logEvent, logEventType, value);
                GtidLogEvent gtidLogEvent = (GtidLogEvent) logEvent;
                gtidLogEvent.setEventType(LogEventType.drc_gtid_log_event.getType());
                transactionCache.add(gtidLogEvent);
                value.setNotRelease(true);
            } else if (xid_log_event == logEventType) {
                circularBreak = false;
                if (value.isBlackTableFiltered() || value.isCircularBreak()) {  // persist xid_log_event, no need fake another one
                    checkXid(logEvent, logEventType, value);
                    transactionCache.add(logEvent);
                    value.setNotRelease(true);
                }
            } else {
                circularBreak = value.isCircularBreak() ? true : false;
                if (circularBreak) {
                    transactionCache.add(logEvent);
                    value.setNotRelease(true);
                }
            }
        } else {
            circularBreak = false;
            checkXid(logEvent, logEventType, value);
            transactionCache.add(logEvent); //persist log event
            value.setNotRelease(true);
        }

        return filtered;
    }

    // set gtid in InboundLogEventContext when receive gtid_log_event and reset receive xid_log_event
    // transaction like ddl without xid, fake a xid_log_event receiving another gtid_log_event
    private void checkXid(LogEvent logEvent, LogEventType logEventType, InboundLogEventContext value) {
        if (gtid_log_event == logEventType) {
            String previousGtid = value.getGtid();
            if (StringUtils.isNotBlank(previousGtid) && previousGtid.contains(":")) {
                XidLogEvent fakeXidLogEvent = new XidLogEvent(FAKE_SERVER_PARAM, FAKE_XID_PARAM, FAKE_XID_PARAM);
                transactionCache.add(fakeXidLogEvent);
                value.unmark(BLACK_TABLE_NAME_F);
            }
            GtidLogEvent gtidLogEvent = (GtidLogEvent) logEvent;
            value.setGtid(gtidLogEvent.getGtid());
        } else if (xid_log_event == logEventType) {
            value.setGtid(StringUtils.EMPTY);
        }
    }

    @Override
    public void reset() {
        circularBreak = false;
        super.reset();
    }

}
