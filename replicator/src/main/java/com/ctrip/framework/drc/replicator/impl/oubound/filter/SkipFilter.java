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

import java.util.Objects;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.drc_gtid_log_event;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.xid_log_event;
import static com.ctrip.framework.drc.core.driver.util.LogEventUtils.isDrcGtidLogEvent;
import static com.ctrip.framework.drc.core.driver.util.LogEventUtils.isOriginGtidLogEvent;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.GTID_LOGGER;

/**
 * Created by jixinwang on 2023/10/12
 */
public abstract class SkipFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private Frequency frequencySend = new Frequency("FRE GTID SEND");

    private final GtidSet excludedSet;

    private ConsumeType consumeType;

    protected String previousGtid = StringUtils.EMPTY;

    protected boolean inExcludeGroup = false;

    private String registerKey;

    public SkipFilter(FilterChainContext context) {
        this.excludedSet = Objects.requireNonNullElseGet(context.getExcludedSet(), () -> new GtidSet(StringUtils.EMPTY));
        this.consumeType = context.getConsumeType();
        this.registerKey = context.getRegisterKey();
    }

    protected abstract void channelHandleEvent(LogEventType eventType);

    protected abstract void skipTransaction(OutboundLogEventContext value, long nextTransactionOffset);

    protected abstract void skipEvent(OutboundLogEventContext value);

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        LogEventType eventType = value.getEventType();

        if (LogEventUtils.isGtidLogEvent(eventType)) {
            handleGtidEvent(value, eventType);
        } else {
            handleNonGtidEvent(value, eventType);
        }

        channelHandleEvent(eventType);

        return doNext(value, value.isSkipEvent());
    }

    protected void handleGtidEvent(OutboundLogEventContext value, LogEventType eventType) {
        value.setEverSeeGtid(true);
        GtidLogEvent gtidLogEvent = value.readGtidEvent();
        value.setLogEvent(gtidLogEvent);
        value.setGtid(gtidLogEvent.getGtid());
        previousGtid = value.getGtid();

        Gtid gtid = Gtid.from(gtidLogEvent);
        inExcludeGroup = skipEvent(excludedSet, eventType, gtid);
        if (inExcludeGroup) {
            GTID_LOGGER.debug("[Skip] gtid log event, gtid:{}, lastCommitted:{}, sequenceNumber:{}, type:{}", value.getGtid(), gtidLogEvent.getLastCommitted(), gtidLogEvent.getSequenceNumber(), eventType);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.outbound.gtid.skip", registerKey);
            value.setSkipEvent(true);
            long nextTransactionOffset = gtidLogEvent.getNextTransactionOffset();
            if (nextTransactionOffset > 0) {
                skipTransaction(value, nextTransactionOffset);
            }
            return;
        } else {
            synchronized (excludedSet) {
                excludedSet.addAndFillGap(gtid.getUuid(), gtid.getTransactionId());
            }
        }

        if (drc_gtid_log_event == eventType && !consumeType.requestAllBinlog()) {
            value.setSkipEvent(true);
            inExcludeGroup = true;
        }
    }

    private void handleNonGtidEvent(OutboundLogEventContext value, LogEventType eventType) {
        if (inExcludeGroup && !LogEventUtils.isSlaveConcerned(eventType)) {
            skipEvent(value);
            value.setSkipEvent(true);
            //skip all transaction, clear in_exclude_group
            if (xid_log_event == eventType) {
                GTID_LOGGER.debug("[Reset] in_exclude_group to false, gtid:{}", previousGtid);
                inExcludeGroup = false;
            }
        }
    }

    private boolean skipEvent(GtidSet excludedSet, LogEventType eventType, Gtid gtid) {
        if (LogEventUtils.isGtidLogEvent(eventType)) {
            return gtid.isContainedWithin(excludedSet);
        }
        return inExcludeGroup;
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
