package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcUuidLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.framework.drc.core.server.observer.uuid.UuidObservable;
import com.ctrip.framework.drc.core.server.observer.uuid.UuidObserver;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.utils.VisibleForTesting;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.gtid_log_event;
import static com.ctrip.framework.drc.replicator.impl.inbound.filter.TransactionFlags.GTID_F;
import static com.ctrip.framework.drc.replicator.impl.inbound.filter.TransactionFlags.OTHER_F;

/**
 * Created by mingdongli
 * 2019/10/9 上午10:17.
 */
public class UuidFilter extends AbstractLogEventFilter<InboundLogEventContext> implements UuidObserver {

    private Set<UUID> whiteList;

    public UuidFilter(Set<UUID> whiteList) {
        this.whiteList = whiteList;
    }

    @Override
    public boolean doFilter(InboundLogEventContext value) {
        LogEvent logEvent = value.getLogEvent();
        if (logEvent instanceof GtidLogEvent) {
            GtidLogEvent gtidLogEvent = (GtidLogEvent) logEvent;
            LogEventType logEventType = gtidLogEvent.getLogEventType();
            value.reset();
            if (gtid_log_event == logEventType) {
                UUID uuid = gtidLogEvent.getServerUUID();
                boolean skip = !whiteList.contains(uuid);
                if (skip) {
                    value.mark(GTID_F);
                }
            }
        } else if (logEvent instanceof DrcUuidLogEvent) {
            DrcUuidLogEvent uuidLogEvent = (DrcUuidLogEvent) logEvent;
            Set<String> uuids = uuidLogEvent.getUuids();
            if (uuids != null && !uuids.isEmpty()) {
                String previousUuids = whiteList.toString();
                this.whiteList.addAll(uuids.stream().map(uuid -> UUID.fromString(uuid)).collect(Collectors.toSet()));
                value.mark(OTHER_F);
                logger.info("[Uuids] update from {} to {}", previousUuids, whiteList);
            }
        }
        return doNext(value, value.isInExcludeGroup());
    }

    @Override
    public void update(Object args, Observable observable) {  //update uuid
        if (observable instanceof UuidObservable) {
            whiteList.addAll((Set<UUID>) args);
        }
    }

    @VisibleForTesting
    public Set<UUID> getWhiteList() {
        return Collections.unmodifiableSet(whiteList);
    }
}
