package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.monitor.log.Frequency;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Objects;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.*;
import static com.ctrip.framework.drc.core.driver.util.LogEventUtils.isDrcGtidLogEvent;
import static com.ctrip.framework.drc.core.driver.util.LogEventUtils.isOriginGtidLogEvent;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.GTID_LOGGER;

/**
 * Created by jixinwang on 2023/10/12
 */
public class SkipFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private Frequency frequencySend = new Frequency("FRE GTID SEND");

    private GtidSet excludedSet;

    private boolean skipDrcGtidLogEvent;

    private ConsumeType consumeType;

    private String previousGtid = StringUtils.EMPTY;

    private String registerKey;

    public SkipFilter(OutboundFilterChainContext context) {
        this.excludedSet = Objects.requireNonNullElseGet(context.getExcludedSet(), () -> new GtidSet(StringUtils.EMPTY));
        this.skipDrcGtidLogEvent = context.isSkipDrcGtidLogEvent();
        this.consumeType = context.getConsumeType();
        this.registerKey = context.getRegisterKey();
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {

        LogEventType eventType = value.getEventType();

        if (LogEventUtils.isGtidLogEvent(eventType)) {
            value.setEverSeeGtid(true);
            GtidLogEvent gtidLogEvent = value.readGtidEvent();
            value.setLogEvent(gtidLogEvent);
            value.setGtid(gtidLogEvent.getGtid());
            boolean inExcludeGroup = skipEvent(excludedSet, eventType, gtidLogEvent.getGtid(), value.isInExcludeGroup());
            value.setInExcludeGroup(inExcludeGroup);
            String newGtidForLog = gtidLogEvent.getGtid();
            previousGtid = newGtidForLog;
            if (inExcludeGroup) {
                GTID_LOGGER.info("[Skip] gtid log event, gtid:{}, lastCommitted:{}, sequenceNumber:{}, type:{}", newGtidForLog, gtidLogEvent.getLastCommitted(), gtidLogEvent.getSequenceNumber(), eventType);
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.outbound.gtid.skip", registerKey);
                value.setSkipEvent(true);
                long nextTransactionOffset = gtidLogEvent.getNextTransactionOffset();
                if (nextTransactionOffset > 0) {
                    FileChannel fileChannel = value.getFileChannel();
                    try {
                        fileChannel.position(fileChannel.position() + nextTransactionOffset);
                        value.setInExcludeGroup(false);

                    } catch (IOException e) {
                        value.setCause(new Exception("position error"));
                    }
                }
            }

            if (drc_gtid_log_event == eventType && !consumeType.requestAllBinlog()) {
                value.setInExcludeGroup(true);
            }

        } else {
            boolean isSlaveConcerned = LogEventUtils.isSlaveConcerned(eventType);
            if (!isSlaveConcerned && value.isInExcludeGroup()) {
                value.setSkipEvent(true);

                //skip all transaction, clear in_exclude_group
                if (xid_log_event == eventType) {
                    GTID_LOGGER.info("[Reset] in_exclude_group to false, gtid:{}", previousGtid);
                    value.setInExcludeGroup(false);
                }
            }
        }

        if (!value.isInExcludeGroup()) {
            logGtid(previousGtid, eventType);
        }

        return doNext(value, value.isSkipEvent());
    }

    private boolean skipEvent(GtidSet excludedSet, LogEventType eventType, String gtid, boolean inExcludeGroup) {
        if (eventType == gtid_log_event) {
            return new GtidSet(gtid).isContainedWithin(excludedSet);
        }

        if (eventType == drc_gtid_log_event) {
            return skipDrcGtidLogEvent || new GtidSet(gtid).isContainedWithin(excludedSet);
        }
        return inExcludeGroup;
    }

    private void logGtid(String gtidForLog, LogEventType eventType) {
        if (xid_log_event == eventType) {
            GTID_LOGGER.debug("[S] X, {}", gtidForLog);
        } else if (isOriginGtidLogEvent(eventType)) {
            frequencySend.addOne();
            if (StringUtils.isNotBlank(gtidForLog)) {
                GTID_LOGGER.info("[S] G, {}", gtidForLog);
            }
        }  else if (isDrcGtidLogEvent(eventType)) {
            frequencySend.addOne();
            if (StringUtils.isNotBlank(gtidForLog)) {
                GTID_LOGGER.info("[S] drc G, {}", gtidForLog);
            }
        } else if (LogEventUtils.isDrcTableMapLogEvent(eventType)) {
            GTID_LOGGER.info("[S] drc table map, {}", gtidForLog);
        } else if (LogEventUtils.isDrcDdlLogEvent(eventType)) {
            GTID_LOGGER.info("[S] drc ddl, {}", gtidForLog);
        }
    }
}
