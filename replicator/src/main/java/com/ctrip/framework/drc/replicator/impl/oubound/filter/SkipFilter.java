package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Objects;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.*;

/**
 * Created by jixinwang on 2023/10/12
 */
public class SkipFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private GtidSet excludedSet;

    private boolean skipDrcGtidLogEvent;

    private ConsumeType consumeType;

    public SkipFilter(OutboundFilterChainContext context) {
        this.excludedSet = Objects.requireNonNullElseGet(context.getExcludedSet(), () -> new GtidSet(StringUtils.EMPTY));
        this.skipDrcGtidLogEvent = context.isSkipDrcGtidLogEvent();
        this.consumeType = context.getConsumeType();
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
            if (inExcludeGroup) {
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
                    value.setInExcludeGroup(false);
                }
            }
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
}
