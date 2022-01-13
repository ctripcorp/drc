package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.activity.replicator.converter.TransactionTableApplierByteBufConverter;
import com.ctrip.framework.drc.applier.activity.replicator.driver.ApplierPooledConnector;
import com.ctrip.framework.drc.applier.event.ApplierDrcGtidEvent;
import com.ctrip.framework.drc.applier.event.ApplierFormatDescriptionEvent;
import com.ctrip.framework.drc.applier.resource.TransactionTable;
import com.ctrip.framework.drc.fetcher.activity.replicator.FetcherSlaveServer;
import com.ctrip.framework.drc.fetcher.event.FetcherEvent;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;

/**
 * Created by jixinwang on 2021/8/20
 */
public class TransactionTableApplierDumpEventActivity extends ApplierDumpEventActivity {

    private boolean skipEvent;

    @InstanceResource
    public TransactionTable transactionTable;

    @Override
    protected FetcherSlaveServer getFetcherSlaveServer() {
        return new FetcherSlaveServer(config, new ApplierPooledConnector(config.getEndpoint()), new TransactionTableApplierByteBufConverter());
    }

    @Override
    protected void doHandleLogEvent(FetcherEvent event) {
        skipEvent = false;

        if (event instanceof ApplierDrcGtidEvent) {
            transactionTable.recordOppositeGtid(((ApplierDrcGtidEvent) event).getGtid());
            skipEvent = true;
            return;
        }

        if (event instanceof ApplierFormatDescriptionEvent) {
            transactionTable.mergeOppositeGtid(true);
            skipEvent = true;
            return;
        }

        super.doHandleLogEvent(event);
    }

    @Override
    public void doStart() throws Exception {
        transactionTable.mergeRecordsFromDB();
        super.doStart();
    }

    @Override
    protected boolean shouldSkip() {
        return skipEvent;
    }

    @Override
    public void doDispose() throws Exception{
        super.doDispose();
        transactionTable.mergeOppositeGtid(false);
    }
}
