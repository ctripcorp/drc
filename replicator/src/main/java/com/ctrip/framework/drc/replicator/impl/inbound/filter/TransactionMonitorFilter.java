package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.monitor.kpi.InboundMonitorReport;
import com.ctrip.framework.drc.core.monitor.log.Frequency;
import com.ctrip.framework.drc.core.server.common.filter.AbstractPostLogEventFilter;
import org.apache.commons.lang3.StringUtils;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.*;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.EVENT_LOGGER;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.GTID_LOGGER;

/**
 * @Author limingdong
 * @create 2020/4/24
 */
public class TransactionMonitorFilter extends AbstractPostLogEventFilter<LogEventInboundContext> {

    private Frequency frequencyReceive = new Frequency("FRE GTID RECEIVE");

    private InboundMonitorReport inboundMonitorReport;

    private String currentGtid;

    public TransactionMonitorFilter(InboundMonitorReport inboundMonitorReport) {
        this.inboundMonitorReport = inboundMonitorReport;
    }

    @Override
    public boolean doFilter(LogEventInboundContext value) {

        LogEvent logEvent = value.getLogEvent();

        //monitor size
        long eventSize = logEvent.getLogEventHeader().getEventSize();
        inboundMonitorReport.addSize(eventSize);

        LogEventType logEventType = logEvent.getLogEventType();

        if (gtid_log_event == logEventType || drc_gtid_log_event == logEventType) {
            GtidLogEvent gtidLogEvent = (GtidLogEvent) logEvent;
            currentGtid = gtidLogEvent.getGtid();
            GTID_LOGGER.info("[R] G:{}", currentGtid);
            //monitor count
            inboundMonitorReport.addOneCount();
            frequencyReceive.addOne();
            inboundMonitorReport.addInboundGtid(1);

        }
        if (EVENT_LOGGER.isDebugEnabled()) {
            EVENT_LOGGER.debug("[Receive] {} for {}", currentGtid, logEventType);
        }

        boolean filtered = doNext(value, value.isInExcludeGroup());  //before filter after

        if (gtid_log_event == logEventType || drc_gtid_log_event == logEventType) {
            String currentGtid = ((GtidLogEvent)logEvent).getGtid();

            if (filtered || drc_gtid_log_event == logEventType) {
                inboundMonitorReport.addGtidFilter(1);
            } else {
                inboundMonitorReport.addGtidStore(1, currentGtid);
            }

            String previousGtid = value.getGtid();
            if (StringUtils.isNotBlank(previousGtid) && previousGtid.contains(":")) {
                GTID_LOGGER.info("[Fake] xid log event for gtid {}", previousGtid);
                inboundMonitorReport.addXidFake(1);
            }
        } else if (xid_log_event == logEventType){
            inboundMonitorReport.addInboundXid(1);
        }

        return filtered;
    }

}
