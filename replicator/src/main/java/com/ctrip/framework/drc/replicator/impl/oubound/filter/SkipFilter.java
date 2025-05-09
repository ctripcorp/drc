package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.gtid.Gtid;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.monitor.log.Frequency;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.context.FilterChainContext;
import org.apache.commons.lang3.StringUtils;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.*;
import static com.ctrip.framework.drc.core.driver.util.LogEventUtils.isDrcGtidLogEvent;
import static com.ctrip.framework.drc.core.driver.util.LogEventUtils.isOriginGtidLogEvent;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.GTID_LOGGER;

/**
 * Created by jixinwang on 2023/10/12
 */
public abstract class SkipFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private Frequency frequencySend = new Frequency("FRE GTID SEND");

    private ConsumeType consumeType;

    protected String previousGtid = StringUtils.EMPTY;

    private String registerKey;

    public SkipFilter(FilterChainContext context) {
        this.consumeType = context.getConsumeType();
        this.registerKey = context.getRegisterKey();
    }

    protected abstract void channelHandleEvent(OutboundLogEventContext value, LogEventType eventType);

    protected abstract void skipTransaction(OutboundLogEventContext value, long nextTransactionOffset);

    protected abstract void skipEvent(OutboundLogEventContext value);

    protected abstract GtidSet getExcludedGtidSet();


    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        LogEventType eventType = value.getEventType();

        if (LogEventUtils.isGtidLogEvent(eventType)) {
            handleGtidEvent(value, eventType);
        } else {
            handleNonGtidEvent(value, eventType);
        }

        channelHandleEvent(value, eventType);

        return doNext(value, value.isSkipEvent());
    }

    protected void handleGtidEvent(OutboundLogEventContext value, LogEventType eventType) {
        value.setEverSeeGtid(true);
        GtidLogEvent gtidLogEvent = value.readGtidEvent();
        value.setLogEvent(gtidLogEvent);
        value.setGtid(gtidLogEvent.getGtid());
        previousGtid = value.getGtid();

        Gtid gtid = Gtid.from(gtidLogEvent);
        this.skipEvent(value, eventType, gtid);
        if (value.isInGtidExcludeGroup()) {
            GTID_LOGGER.debug("[Skip] gtid log event, gtid:{}, lastCommitted:{}, sequenceNumber:{}, type:{}", value.getGtid(), gtidLogEvent.getLastCommitted(), gtidLogEvent.getSequenceNumber(), eventType);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.outbound.gtid.skip", registerKey);
            value.setSkipEvent(true);
            long nextTransactionOffset = gtidLogEvent.getNextTransactionOffset();
            if (nextTransactionOffset > 0) {
                skipTransaction(value, nextTransactionOffset);
            }
        }
    }

    private void handleNonGtidEvent(OutboundLogEventContext value, LogEventType eventType) {
        if (value.isInGtidExcludeGroup() && !LogEventUtils.isSlaveConcerned(eventType)) {
            skipEvent(value);
            value.setSkipEvent(true);
            //skip all transaction, clear in_exclude_group
            if (xid_log_event == eventType) {
                GTID_LOGGER.debug("[Reset] in_exclude_group to false, gtid:{}", previousGtid);
                value.setInGtidExcludeGroup(false);
            }
        }
    }

    private void skipEvent(OutboundLogEventContext value, LogEventType eventType, Gtid gtid) {
        if (drc_gtid_log_event == eventType && !consumeType.requestAllBinlog()) {
            value.setInGtidExcludeGroup(true);
            return;
        }
        if (LogEventUtils.isGtidLogEvent(eventType)) {
            GtidSet excludedSet = this.getExcludedGtidSet();
            value.setInGtidExcludeGroup(gtid.isContainedWithin(excludedSet));
        }
    }

    protected void logGtid(String gtidForLog, LogEventType eventType) {
        if (xid_log_event == eventType) {
            GTID_LOGGER.debug("{}: [S] X, {} ", registerKey, gtidForLog);
        } else if (isOriginGtidLogEvent(eventType)) {
            frequencySend.addOne();
            if (StringUtils.isNotBlank(gtidForLog)) {
                GTID_LOGGER.debug("{}: [S] G, {}", registerKey, gtidForLog);
            }
        } else if (isDrcGtidLogEvent(eventType)) {
            frequencySend.addOne();
            if (StringUtils.isNotBlank(gtidForLog)) {
                GTID_LOGGER.debug("{}: [S] drc G, {} ", registerKey, gtidForLog);
            }
        } else if (LogEventUtils.isDrcTableMapLogEvent(eventType)) {
            GTID_LOGGER.debug("{}: [S] drc table map, {} ", registerKey, gtidForLog);
        } else if (LogEventUtils.isDrcDdlLogEvent(eventType)) {
            GTID_LOGGER.debug("{}: [S] drc ddl, {} ", registerKey, gtidForLog);
        }
    }
}
