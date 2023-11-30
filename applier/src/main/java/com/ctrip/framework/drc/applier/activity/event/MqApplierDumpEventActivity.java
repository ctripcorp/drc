package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.activity.replicator.converter.MqAbstractByteBufConverter;
import com.ctrip.framework.drc.applier.activity.replicator.driver.ApplierPooledConnector;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.fetcher.activity.replicator.FetcherSlaveServer;
import com.ctrip.framework.drc.fetcher.event.ApplierGtidEvent;
import com.ctrip.framework.drc.fetcher.event.FetcherEvent;
import com.ctrip.framework.drc.fetcher.resource.context.MqPosition;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;

import java.util.List;

/**
 * Created by jixinwang on 2022/10/12
 */
public class MqApplierDumpEventActivity extends ApplierDumpEventActivity {

    @InstanceResource
    public MqPosition mqPosition;

    private String lastUuid;

    private long lastTrxId = 0;

    @Override
    protected FetcherSlaveServer getFetcherSlaveServer() {
        return new FetcherSlaveServer(config, new ApplierPooledConnector(config.getEndpoint()), new MqAbstractByteBufConverter());
    }

    @Override
    protected void handleApplierGtidEvent(FetcherEvent event) {
        ApplierGtidEvent applierGtidEvent = (ApplierGtidEvent) event;
        String currentUuid = applierGtidEvent.getServerUUID().toString();
        long trxId = applierGtidEvent.getId();

        if (!currentUuid.equalsIgnoreCase(lastUuid)) {
            lastUuid = currentUuid;
            compensateExecutedGtidSetGap(currentUuid, trxId);
        } else if (trxId > lastTrxId + 1) {
            compensateGtidSetGap(currentUuid, lastTrxId, trxId);
        }
        lastTrxId = trxId;

        super.handleApplierGtidEvent(event);
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
            mqPosition.updatePosition(gtid);
        }
    }
}
