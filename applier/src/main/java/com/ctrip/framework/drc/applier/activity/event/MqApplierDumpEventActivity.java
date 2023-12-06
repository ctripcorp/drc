package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.activity.replicator.converter.MqAbstractByteBufConverter;
import com.ctrip.framework.drc.applier.activity.replicator.driver.ApplierPooledConnector;
import com.ctrip.framework.drc.fetcher.activity.replicator.FetcherSlaveServer;
import com.ctrip.framework.drc.fetcher.resource.context.MqPosition;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;

/**
 * Created by jixinwang on 2022/10/12
 */
public class MqApplierDumpEventActivity extends ApplierDumpEventActivity {

    @InstanceResource
    public MqPosition mqPosition;

    @Override
    protected FetcherSlaveServer getFetcherSlaveServer() {
        return new FetcherSlaveServer(config, new ApplierPooledConnector(config.getEndpoint()), new MqAbstractByteBufConverter());
    }

    @Override
    protected void initGap() {
        super.initGap();
        mqPosition.union(toCompensateGtidSet);
    }

    @Override
    protected void addPosition(String gtid) {
        mqPosition.add(gtid);
    }
}
