package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.activity.replicator.converter.TransactionTableApplierByteBufConverter;
import com.ctrip.framework.drc.applier.activity.replicator.driver.ApplierPooledConnector;
import com.ctrip.framework.drc.fetcher.event.ApplierDrcGtidEvent;
import com.ctrip.framework.drc.fetcher.event.ApplierGtidEvent;
import com.ctrip.framework.drc.applier.resource.position.TransactionTable;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.fetcher.activity.replicator.FetcherSlaveServer;
import com.ctrip.framework.drc.fetcher.event.ApplierXidEvent;
import com.ctrip.framework.drc.fetcher.event.FetcherEvent;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.ctrip.framework.drc.applier.resource.position.TransactionTableResource.TRANSACTION_TABLE_SIZE;

/**
 * Created by jixinwang on 2021/8/20
 */
public class TransactionTableApplierDumpEventActivity extends ApplierDumpEventActivity {

    protected final Logger loggerTT = LoggerFactory.getLogger("TRANSACTION TABLE");

    private String lastUuid;

    private int filterCount;

    private boolean needFilter;

    private long lastTrxId = 0;

    @InstanceResource
    public TransactionTable transactionTable;

    @Override
    protected FetcherSlaveServer getFetcherSlaveServer() {
        return new FetcherSlaveServer(config, new ApplierPooledConnector(config.getEndpoint()), new TransactionTableApplierByteBufConverter());
    }

    @Override
    protected void handleApplierDrcGtidEvent(FetcherEvent event) {
        String gtid = ((ApplierDrcGtidEvent) event).getGtid();
        loggerER.info("{} {} - RECEIVED - {}", registryKey, gtid, event.getClass().getSimpleName());
        transactionTable.recordToMemory(gtid);
        updateGtidSet(gtid);
    }

    @Override
    protected void handleApplierGtidEvent(FetcherEvent event) {
        ApplierGtidEvent applierGtidEvent = (ApplierGtidEvent) event;
        String currentUuid = applierGtidEvent.getServerUUID().toString();
        long trxId = applierGtidEvent.getId();
        if (!currentUuid.equalsIgnoreCase(lastUuid)) {
            loggerTT.info("[{}]uuid has changed, old uuid is: {}, new uuid is: {}", registryKey, lastUuid, currentUuid);
            GtidSet gtidSet = transactionTable.mergeRecord(currentUuid, true);
            updateGtidSet(gtidSet);
            lastUuid = currentUuid;
            filterCount = 0;
            needFilter = true;
            compensateExecutedGtidSetGap(currentUuid, trxId);
        } else if (trxId > lastTrxId + 1) {
            compensateGtidSetGap(currentUuid, lastTrxId, trxId);
        }
        lastTrxId = trxId;

        if (needFilter) {
            String gtid =  ((ApplierGtidEvent) event).getGtid();
            GtidSet gtidSet = context.fetchGtidSet();

            if (skipEvent = new GtidSet(gtid).isContainedWithin(gtidSet)) {
                loggerTT.info("[Skip] skip gtid: {}", gtid);
            }
            if (filterCount < TRANSACTION_TABLE_SIZE) {
                filterCount++;
            } else {
                needFilter = false;
                skipEvent = false;
            }
        }

        super.handleApplierGtidEvent(event);
    }

    @Override
    protected boolean shouldSkip(FetcherEvent event) {
        if (event instanceof ApplierDrcGtidEvent) {
            return true;
        }
        if (skipEvent && event instanceof ApplierXidEvent) {
            skipEvent = false;
            return true;
        }
        return skipEvent;
    }

    @VisibleForTesting
    protected void updateGtidSet(String gtid) {
        GtidSet set = context.fetchGtidSet();
        set.add(gtid);
        context.updateGtidSet(set);
    }

    @VisibleForTesting
    protected void updateGtidSet(GtidSet gtidset) {
        GtidSet set = context.fetchGtidSet();
        loggerTT.info("[Skip] update gtidset in db before, context gtidset: {}, merged gtidset in db: {}", set.toString(), gtidset.toString());
        context.updateGtidSet(set.union(gtidset));
        loggerTT.info("[Skip] update gtidset in db after, union result: {}", context.fetchGtidSet().toString());
    }

    @VisibleForTesting
    public boolean isNeedFilter() {
        return needFilter;
    }


    private void compensateExecutedGtidSetGap(String currentUuid, long receivedTrxId) {
        GtidSet.UUIDSet executedUuidSet = context.fetchGtidSet().getUUIDSet(currentUuid);
        if (executedUuidSet == null) {
            return;
        }

        List<GtidSet.Interval> executedIntervals = executedUuidSet.getIntervals();
        if (executedIntervals.isEmpty()) {
            return;
        }

        long maxExecutedTrxId = executedIntervals.get(executedIntervals.size() - 1).getEnd();
        if (receivedTrxId + 1 > maxExecutedTrxId) {
            compensateGtidSetGap(currentUuid, maxExecutedTrxId, receivedTrxId);
        }
    }

    private void compensateGtidSetGap(String uuid, long start, long end) {
        for (long i = start + 1; i < end; i++) {
            String gtid = uuid + ":" + i;
            transactionTable.recordToMemory(gtid);
        }
    }
}
