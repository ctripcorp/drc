package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.activity.replicator.converter.TransactionTableApplierByteBufConverter;
import com.ctrip.framework.drc.applier.activity.replicator.driver.ApplierPooledConnector;
import com.ctrip.framework.drc.applier.event.ApplierDrcGtidEvent;
import com.ctrip.framework.drc.applier.event.ApplierFormatDescriptionEvent;
import com.ctrip.framework.drc.applier.event.ApplierGtidEvent;
import com.ctrip.framework.drc.applier.resource.TransactionTable;
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

    private boolean skipEvent;

    private String lastUuid;

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
            transactionTable.recordGtidInMemory(((ApplierDrcGtidEvent) event).getGtid());
            skipEvent = true;
            return;
        }

        if (event instanceof ApplierFormatDescriptionEvent) {
            transactionTable.asyncMergeGtid(true);
            skipEvent = true;
            return;
        }

        if (event instanceof ApplierGtidEvent) {
            String currentUuid = ((ApplierGtidEvent) event).getServerUUID().toString();
            if (!currentUuid.equalsIgnoreCase(lastUuid)) {
                loggerTT.info("uuid has changed, old uuid is: {}, new uuid is: {}", lastUuid, currentUuid);
                transactionTable.mergeGtidRecordInDB(currentUuid);
                lastUuid = currentUuid;
            }
        }

        super.doHandleLogEvent(event);
    }

    @Override
    protected boolean shouldSkip() {
        return skipEvent;
    }

    @Override
    public void doDispose() throws Exception{
        super.doDispose();
        transactionTable.syncMergeGtid(false);
    }
}
