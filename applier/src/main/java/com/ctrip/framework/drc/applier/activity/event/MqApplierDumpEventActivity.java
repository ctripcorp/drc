package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.activity.replicator.converter.MqAbstractByteBufConverter;
import com.ctrip.framework.drc.applier.activity.replicator.driver.ApplierPooledConnector;
import com.ctrip.framework.drc.fetcher.activity.replicator.FetcherSlaveServer;

/**
 * Created by jixinwang on 2022/10/12
 */
public class MqApplierDumpEventActivity extends ApplierDumpEventActivity {

    @Override
    protected FetcherSlaveServer getFetcherSlaveServer() {
        return new FetcherSlaveServer(config, new ApplierPooledConnector(config.getEndpoint()), new MqAbstractByteBufConverter());
    }
}
