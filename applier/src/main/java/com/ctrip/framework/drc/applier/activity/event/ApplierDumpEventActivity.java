package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.activity.replicator.converter.ApplierByteBufConverter;
import com.ctrip.framework.drc.applier.activity.replicator.driver.ApplierPooledConnector;
import com.ctrip.framework.drc.applier.event.ApplierDrcTableMapEvent;
import com.ctrip.framework.drc.applier.event.ApplierDrcUuidLogEvent;
import com.ctrip.framework.drc.applier.event.ApplierPreviousGtidsLogEvent;
import com.ctrip.framework.drc.applier.resource.condition.Progress;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.LogEventCallBack;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcHeartbeatLogEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.fetcher.activity.event.DumpEventActivity;
import com.ctrip.framework.drc.fetcher.activity.replicator.FetcherSlaveServer;
import com.ctrip.framework.drc.fetcher.event.*;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;
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

    private String lastReceivedUuid;

    protected boolean gapInited = false;

    protected GtidSet.Interval receivedStartAndEndTrxId;

    protected GtidSet toInitGap = new GtidSet(StringUtils.EMPTY);

    private GtidSet previousGtidSet = new GtidSet(StringUtils.EMPTY);

    private Set<String> uuids = Sets.newHashSet();

    //key: uuid, value: last received trxId
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

        if (event instanceof ApplierPreviousGtidsLogEvent) {
            previousGtidSet = ((ApplierPreviousGtidsLogEvent) event).getGtidSet();
        }

        if (event instanceof ApplierDrcUuidLogEvent) {
            ApplierDrcUuidLogEvent uuidLogEvent = (ApplierDrcUuidLogEvent) event;
            uuids =uuidLogEvent.getUuids();
            compensateGapOfDiffUuid(uuidLogEvent);
        }
    }

    @Override
    protected void afterHandleLogEvent(FetcherEvent logEvent) {
        if (shouldSkip(logEvent) || ignoreEvent(logEvent)) {
            logEvent.release();
            if (logEvent instanceof MonitoredGtidLogEvent) {
                capacity.release();
            }
            progress.tick();
        } else {
            super.afterHandleLogEvent(logEvent);
        }
    }

    private boolean ignoreEvent(FetcherEvent event) {
        return event instanceof ApplierPreviousGtidsLogEvent || event instanceof ApplierDrcUuidLogEvent;
    }

    protected void handleApplierDrcGtidEvent(FetcherEvent event) {
        ((ApplierDrcGtidEvent) event).involve(context);
    }

    protected void handleApplierGtidEvent(ApplierGtidEvent event) {
        compensateGapIfNeed(event);
        event.involve(context);
    }

    private void compensateGapOfDiffUuid(ApplierDrcUuidLogEvent uuidLogEvent) {
        uuids = uuidLogEvent.getUuids();
        if (uuids == null || previousGtidSet == null) {
            return;
        }

        for (String uuid : uuids) {
            Long lastTrxId = lastReceivedTxs.get(uuid);
            GtidSet previousGtidSetOfUuid = previousGtidSet.filterGtid(Sets.newHashSet(uuid));
            if (previousGtidSetOfUuid == null || previousGtidSetOfUuid.isContainedWithin(context.fetchGtidSet())) {
                continue;
            }

            GtidSet.Interval previousInterval = getStartAndEnd(previousGtidSetOfUuid, uuid);
            if (previousInterval == null) {
                continue;
            }
            long previousEnd = previousInterval.getEnd();

            if (lastTrxId == null) {
                // merge the all previous gtid set
                compensateGap(previousGtidSetOfUuid);
                lastReceivedTxs.put(uuid, previousEnd);
                DefaultEventMonitorHolder.getInstance().logBatchEvent("Drc.uuid.merge", registryKey + ":" + uuid, 1, 0);
                logger.info("[Merge][Uuid][{}] last null, all gtid set: {}", registryKey, previousGtidSetOfUuid.toString());
            } else {
                // merge the gap in the start
                long previousStart = previousInterval.getStart();
                GtidSet.Interval receivedInterval = getStartAndEnd(context.fetchGtidSet(), uuid);
                if (receivedInterval != null) {
                    long receivedStart = receivedInterval.getStart();
                    if (previousStart < receivedStart) {
                        GtidSet previousGtidSet = new GtidSet(uuid + ":" + previousStart + "-" + (receivedStart - 1));
                        compensateGap(previousGtidSet);
                        DefaultEventMonitorHolder.getInstance().logBatchEvent("Drc.uuid.merge", registryKey + ":" + uuid, 1, 0);
                        logger.info("[Merge][Uuid][{}] start gtid set: {}", registryKey, previousGtidSet.toString());
                    }
                }

                // merge the gap in the end
                if (previousEnd > lastTrxId) {
                    GtidSet gtidSet = new GtidSet(uuid + ":" + (lastTrxId + 1) + "-" + previousEnd);
                    compensateGap(gtidSet);
                    DefaultEventMonitorHolder.getInstance().logBatchEvent("Drc.uuid.merge", registryKey + ":" + uuid, 1, 0);
                    logger.info("[Merge][Uuid][{}] end gtid set: {}", registryKey, gtidSet.toString());
                }
            }
        }
    }

    private void compensateGapIfNeed(ApplierGtidEvent applierGtidEvent) {
        String uuid = applierGtidEvent.getServerUUID().toString();
        long trxId = applierGtidEvent.getId();
        Long lastTrxId = lastReceivedTxs.get(uuid);

        if (!uuid.equalsIgnoreCase(lastReceivedUuid)) {
            gapInited = false;
            receivedStartAndEndTrxId = getStartAndEnd(context.fetchGtidSet(), uuid);
            if (lastTrxId == null) {
                if (receivedStartAndEndTrxId != null) {
                    saveGapToInit(uuid, receivedStartAndEndTrxId.getStart() + 1, trxId - 1);
                }
            } else {
                saveGapToInit(uuid, lastTrxId + 1, trxId - 1);
            }
        } else {
            if (gapInited) {
                if (trxId > lastTrxId + 1) {
                    compensateGap(uuid, lastTrxId + 1, trxId);
                }
            } else {
                if (trxId > lastTrxId + 1) {
                    saveGapToInit(uuid, lastTrxId + 1, trxId - 1);
                }

                if (receivedStartAndEndTrxId == null || trxId >= receivedStartAndEndTrxId.getEnd()) {
                    compensateGap(toInitGap);
                    gapInited = true;
                    clearInitedGap();
                }
            }
        }

        lastReceivedUuid = uuid;
        lastReceivedTxs.put(uuid, trxId);
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

    private void compensateGap(GtidSet gtidSet) {
        updateContextGtidSet(gtidSet);
        persistPosition(gtidSet);
    }

    protected void persistPosition(GtidSet gtidSet) {

    }

    protected void persistPosition(String gtid) {

    }

    private void compensateGap(String uuid, long start, long end) {
        for (long i = start; i < end; i++) {
            String gtid = uuid + ":" + i;
            updateContextGtidSet(gtid);
            persistPosition(gtid);
        }
    }

    private void saveGapToInit(String uuid, long start, long end) {
        if (end > start) {
            toInitGap = toInitGap.union(new GtidSet(uuid + ":" + start + "-" + end));
        } else if (end == start) {
            toInitGap.add(uuid + ":" + end);
        }
    }

    private void clearInitedGap() {
        logger.info("[Merge][{}] clear inited gap: {}", registryKey, toInitGap);
        toInitGap = new GtidSet(StringUtils.EMPTY);
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
