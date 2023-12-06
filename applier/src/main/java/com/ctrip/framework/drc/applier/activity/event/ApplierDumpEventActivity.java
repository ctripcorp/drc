package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.activity.replicator.converter.ApplierByteBufConverter;
import com.ctrip.framework.drc.applier.activity.replicator.driver.ApplierPooledConnector;
import com.ctrip.framework.drc.applier.event.ApplierDrcTableMapEvent;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.fetcher.event.ApplierDrcGtidEvent;
import com.ctrip.framework.drc.fetcher.event.ApplierGtidEvent;
import com.ctrip.framework.drc.fetcher.event.ApplierXidEvent;
import com.ctrip.framework.drc.applier.resource.condition.Progress;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.LogEventCallBack;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcHeartbeatLogEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.fetcher.activity.event.DumpEventActivity;
import com.ctrip.framework.drc.fetcher.activity.replicator.FetcherSlaveServer;
import com.ctrip.framework.drc.fetcher.event.FetcherEvent;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.HEARTBEAT_LOGGER;

/**
 * @Author limingdong
 * @create 2021/3/4
 */
public class ApplierDumpEventActivity extends DumpEventActivity<FetcherEvent> {

    protected final Logger loggerTT = LoggerFactory.getLogger("TRANSACTION TABLE");

    @InstanceResource
    public Progress progress;

    public boolean skipEvent = false;

    private String lastUuid;

    private long lastTrxId;

    protected boolean gapInited = false;

    protected GtidSet.Interval startAndEndTrxId;

    protected GtidSet toCompensateGtidSet = new GtidSet(StringUtils.EMPTY);

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
            handleApplierGtidEvent(event);
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

    protected void handleApplierGtidEvent(FetcherEvent event) {
        ApplierGtidEvent applierGtidEvent = (ApplierGtidEvent) event;
        checkPositionGap(applierGtidEvent);
        applierGtidEvent.involve(context);
    }

    private void checkPositionGap(ApplierGtidEvent applierGtidEvent) {
        String currentUuid = applierGtidEvent.getServerUUID().toString();
        long trxId = applierGtidEvent.getId();

        if (!currentUuid.equalsIgnoreCase(lastUuid)) {
            lastUuid = currentUuid;
            gapInited = false;
            startAndEndTrxId = getStartAndEnd(context.fetchGtidSet(), currentUuid);
            if (startAndEndTrxId != null) {
                unionExecutedGtidSetGap(currentUuid, startAndEndTrxId.getStart(), trxId - 1);
            }
        } else {
            initGapIfNeed(currentUuid, trxId);
            if (trxId > lastTrxId + 1) {
                compensateGtidSetGap(currentUuid, lastTrxId, trxId);
            }
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
            return;
        }

        if (trxId < startAndEndTrxId.getEnd()) {
            unionExecutedGtidSetGap(uuid, lastTrxId + 1, trxId - 1);
        } else {
            initGap();
            gapInited = true;
            clearExecutedGtidSetGap();
        }
    }

    protected void initGap() {
        updateGtidSet(toCompensateGtidSet);
    }

    protected void addPosition(String gtid) {

    }

    private void compensateGtidSetGap(String uuid, long start, long end) {
        for (long i = start + 1; i < end; i++) {
            String gtid = uuid + ":" + i;
            addPosition(gtid);
        }
    }

    protected void unionExecutedGtidSetGap(String uuid, long start, long end) {
        if (end > start) {
            return;
        }
        String toCompensateGtidSetString = uuid + ":" + start + "-" + end;
        toCompensateGtidSet = toCompensateGtidSet.union(new GtidSet(toCompensateGtidSetString));
    }

    protected GtidSet getExecutedGtidSetGap() {
        return toCompensateGtidSet;
    }

    protected void clearExecutedGtidSetGap() {
        toCompensateGtidSet = new GtidSet(Strings.EMPTY);
    }

    protected GtidSet.Interval getStartAndEnd(GtidSet gtidSet, String uuid) {
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

    @VisibleForTesting
    protected void updateGtidSet(GtidSet gtidset) {
        GtidSet set = context.fetchGtidSet();
        loggerTT.info("[Skip] update gtidset in db before, context gtidset: {}, merged gtidset in db: {}", set.toString(), gtidset.toString());
        context.updateGtidSet(set.union(gtidset));
        loggerTT.info("[Skip] update gtidset in db after, union result: {}", context.fetchGtidSet().toString());
    }
}
