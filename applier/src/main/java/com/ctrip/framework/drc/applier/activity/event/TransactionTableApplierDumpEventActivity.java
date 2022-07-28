package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.activity.replicator.converter.TransactionTableApplierByteBufConverter;
import com.ctrip.framework.drc.applier.activity.replicator.driver.ApplierPooledConnector;
import com.ctrip.framework.drc.fetcher.event.ApplierDrcGtidEvent;
import com.ctrip.framework.drc.fetcher.event.ApplierGtidEvent;
import com.ctrip.framework.drc.applier.resource.position.TransactionTable;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.fetcher.activity.replicator.FetcherSlaveServer;
import com.ctrip.framework.drc.fetcher.event.FetcherEvent;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jixinwang on 2021/8/20
 */
public class TransactionTableApplierDumpEventActivity extends ApplierDumpEventActivity {

    protected final Logger loggerTT = LoggerFactory.getLogger("TRANSACTION TABLE");

    private String lastUuid;

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
        String currentUuid = ((ApplierGtidEvent) event).getServerUUID().toString();
        if (!currentUuid.equalsIgnoreCase(lastUuid)) {
            loggerTT.info("[{}]uuid has changed, old uuid is: {}, new uuid is: {}", registryKey, lastUuid, currentUuid);
            transactionTable.mergeRecord(currentUuid, true);
            lastUuid = currentUuid;
        }

        super.handleApplierGtidEvent(event);
    }

    @Override
    protected boolean shouldSkip(FetcherEvent event) {
        return (event instanceof ApplierDrcGtidEvent);
    }

    protected void updateGtidSet(String gtid) {
        GtidSet set = context.fetchGtidSet();
        set.add(gtid);
        context.updateGtidSet(set);
    }
}
