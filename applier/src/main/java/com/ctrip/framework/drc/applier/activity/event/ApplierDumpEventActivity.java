package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.activity.replicator.converter.ApplierByteBufConverter;
import com.ctrip.framework.drc.applier.activity.replicator.driver.ApplierPooledConnector;
import com.ctrip.framework.drc.applier.event.ApplierDrcTableMapEvent;
import com.ctrip.framework.drc.applier.resource.condition.Progress;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.LogEventCallBack;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcHeartbeatLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcUuidLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.PreviousGtidsLogEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.fetcher.activity.event.DumpEventActivity;
import com.ctrip.framework.drc.fetcher.activity.replicator.FetcherSlaveServer;
import com.ctrip.framework.drc.fetcher.event.ApplierDrcGtidEvent;
import com.ctrip.framework.drc.fetcher.event.ApplierGtidEvent;
import com.ctrip.framework.drc.fetcher.event.ApplierXidEvent;
import com.ctrip.framework.drc.fetcher.event.FetcherEvent;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.HEARTBEAT_LOGGER;

/**
 * @Author limingdong
 * @create 2021/3/4
 */
public class ApplierDumpEventActivity extends DumpEventActivity<FetcherEvent> {

    @InstanceResource
    public Progress progress;

    public boolean skipEvent = false;

    private String lastUuid;

    private long lastTrxId;

    protected boolean gapInited = false;

    protected GtidSet.Interval startAndEndTrxId;

    protected GtidSet toCompensateGtidSet = new GtidSet(StringUtils.EMPTY);

    private GtidSet previousGtidSet = new GtidSet(StringUtils.EMPTY);

    private Set<String> uuids = Sets.newHashSet();

    //key: uuid, value: last received gtid
    private Map<String, Long> lastReceivedTxs = Maps.newHashMap();

    @Override
    protected FetcherSlaveServer getFetcherSlaveServer() {
        return new FetcherSlaveServer(config, new ApplierPooledConnector(config.getEndpoint()), new ApplierByteBufConverter());
    }

    @Override
    protected void doHandleLogEvent(FetcherEvent event) {
        if (event instanceof ApplierDrcGtidEvent) {
            handleApplierDrcGtidEvent(event);
            return;
        }

        if (event instanceof ApplierGtidEvent) {
            ApplierGtidEvent applierGtidEvent = (ApplierGtidEvent) event;
            handleApplierGtidEvent(applierGtidEvent);
            return;
        }

        if (event instanceof ApplierXidEvent) {
            ((ApplierXidEvent) event).updateDumpPosition(context);
            return;
        }

        if (event instanceof ApplierDrcTableMapEvent) {
            ApplierDrcTableMapEvent drcEvent = (ApplierDrcTableMapEvent) event;
            loggerER.info("- DRC - {}: {}", drcEvent.getSchemaNameDotTableName(),
                    Columns.from(drcEvent.getColumns()).getNames());
        }

        if (event instanceof PreviousGtidsLogEvent) {
            previousGtidSet = ((PreviousGtidsLogEvent) event).getGtidSet();
            event.release();
        }

        if (event instanceof DrcUuidLogEvent) {
            uuids = ((DrcUuidLogEvent) event).getUuids();
            DrcUuidLogEvent uuidLogEvent = (DrcUuidLogEvent) event;
            unionGtidSetOfUuids(uuidLogEvent);
            event.release();
        }
    }

    @Override
    protected void afterHandleLogEvent(FetcherEvent logEvent) {
        if (shouldSkip(logEvent)) {
            logEvent.release();
            capacity.release();
            progress.tick();
        } else {
            super.afterHandleLogEvent(logEvent);
        }
    }

    protected void handleApplierDrcGtidEvent(FetcherEvent event) {
        ((ApplierDrcGtidEvent) event).involve(context);
    }

    protected void handleApplierGtidEvent(ApplierGtidEvent event) {
        String currentUuid = event.getServerUUID().toString();
        lastReceivedTxs.put(currentUuid, event.getId());
        checkPositionGap(event, currentUuid);
        event.involve(context);
    }

    private void unionGtidSetOfUuids(DrcUuidLogEvent uuidLogEvent) {
        uuids = uuidLogEvent.getUuids();
        if (uuids == null || previousGtidSet == null) {
            return;
        }

        for (String uuid : uuids) {
            Long lastTrxId = lastReceivedTxs.get(uuid);
            GtidSet.Interval interval = getStartAndEnd(previousGtidSet, uuid);
            if (interval == null) {
                continue;
            }
            long end = interval.getEnd();

            if (lastTrxId == null) {
                long start = interval.getStart();
                GtidSet gtidSet = new GtidSet(uuid + ":" + start + "-" + end);
                unionGtidSetGap(gtidSet);
                lastReceivedTxs.put(uuid, end);
                logger.info("[Merge][Uuid][{}] last null, gtid set: {}", registryKey, gtidSet.toString());
            } else {
                long start = lastTrxId;
                if (end > start) {
                    GtidSet gtidSet = new GtidSet(uuid + ":" + (start + 1) + "-" + end);
                    unionGtidSetGap(gtidSet);
                    logger.info("[Merge][Uuid][{}] gtid set: {}", registryKey, gtidSet.toString());
                }
            }
        }
    }

    @VisibleForTesting
    protected void checkPositionGap(ApplierGtidEvent applierGtidEvent, String currentUuid) {
        long trxId = applierGtidEvent.getId();

        if (!currentUuid.equalsIgnoreCase(lastUuid)) {
            lastUuid = currentUuid;
            gapInited = false;
            startAndEndTrxId = getStartAndEnd(context.fetchGtidSet(), currentUuid);
            if (startAndEndTrxId != null) {
                unionExecutedGtidSetGap(currentUuid, startAndEndTrxId.getStart(), trxId - 1);
            }
        } else {
            if (gapInited && trxId > lastTrxId + 1) {
                compensateGtidSetGap(currentUuid, lastTrxId, trxId);
            }
            initGapIfNeed(currentUuid, trxId);
        }
        lastTrxId = trxId;
    }

    protected boolean shouldSkip(FetcherEvent logEvent) {
        return false;
    }

    @Override
    protected boolean heartBeat(LogEvent logEvent, LogEventCallBack logEventCallBack) {
        if (logEvent instanceof DrcHeartbeatLogEvent) {
            HEARTBEAT_LOGGER.info("{} - RECEIVED - {}", registryKey, logEvent.getClass().getSimpleName());
            DrcHeartbeatLogEvent heartbeatLogEvent = (DrcHeartbeatLogEvent) logEvent;
            if (heartbeatLogEvent.shouldTouchProgress()) {
                progress.tick();
                HEARTBEAT_LOGGER.info("{} - Tick - {}", registryKey, logEvent.getClass().getSimpleName());
            }
            logEventCallBack.onHeartHeat();
            try {
                logEvent.release();
            } catch (Exception e) {
                logger.error("[Release] heartbeat event error");
            }
            return true;
        }
        return false;
    }

    private void initGapIfNeed(String uuid, long trxId) {
        if (gapInited) {
            return;
        }

        if (startAndEndTrxId == null) {
            gapInited = true;
            return;
        }

        unionExecutedGtidSetGap(uuid, lastTrxId + 1, trxId - 1);
        if (trxId >= startAndEndTrxId.getEnd()) {
            unionGtidSetGap(toCompensateGtidSet);
            gapInited = true;
            clearExecutedGtidSetGap();
        }
    }

    private void unionGtidSetGap(GtidSet gtidSet) {
        updateContextGtidSet(gtidSet);
        persistPosition(gtidSet);
    }

    protected void persistPosition(GtidSet gtidSet) {

    }

    protected void addPosition(String gtid) {

    }

    private void compensateGtidSetGap(String uuid, long start, long end) {
        for (long i = start + 1; i < end; i++) {
            String gtid = uuid + ":" + i;
            updateContextGtidSet(gtid);
            addPosition(gtid);
        }
    }

    private void unionExecutedGtidSetGap(String uuid, long start, long end) {
        if (end < start) {
            return;
        }
        String toCompensateGtidSetString = uuid + ":" + start + "-" + end;
        toCompensateGtidSet = toCompensateGtidSet.union(new GtidSet(toCompensateGtidSetString));
    }

    private void clearExecutedGtidSetGap() {
        toCompensateGtidSet = new GtidSet(StringUtils.EMPTY);
        logger.info("[Merge][{}] clear toCompensateGtidSet", registryKey);
    }

    private GtidSet.Interval getStartAndEnd(GtidSet gtidSet, String uuid) {
        GtidSet.UUIDSet executedUuidSet = gtidSet.getUUIDSet(uuid);
        if (executedUuidSet == null) {
            return null;
        }

        List<GtidSet.Interval> executedIntervals = executedUuidSet.getIntervals();
        if (executedIntervals.isEmpty()) {
            return null;
        }

        long start = executedIntervals.get(0).getStart();
        long end = executedIntervals.get(executedIntervals.size() - 1).getEnd();
        return new GtidSet.Interval(start, end);
    }

    protected void updateContextGtidSet(GtidSet gtidset) {
        GtidSet set = context.fetchGtidSet();
        context.updateGtidSet(set.union(gtidset));
        logger.info("[Merge][{}] update context gtidset, context gtidset: {}, to merge gtidset: {}, unioned gtidset: {}", registryKey, set.toString(), gtidset.toString(), context.fetchGtidSet().toString());
    }

    protected void updateContextGtidSet(String gtid) {
        GtidSet set = context.fetchGtidSet();
        set.add(gtid);
    }
}
