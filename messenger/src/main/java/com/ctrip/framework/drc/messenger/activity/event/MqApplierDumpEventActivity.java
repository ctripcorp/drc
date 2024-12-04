package com.ctrip.framework.drc.messenger.activity.event;

import com.ctrip.framework.drc.messenger.activity.replicator.converter.MqAbstractByteBufConverter;
import com.ctrip.framework.drc.messenger.activity.replicator.driver.MqPooledConnector;
import com.ctrip.framework.drc.core.driver.binlog.gtid.Gtid;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.fetcher.activity.event.FetcherDumpEventActivity;
import com.ctrip.framework.drc.fetcher.activity.replicator.FetcherSlaveServer;
import com.ctrip.framework.drc.fetcher.resource.context.MqPosition;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;

/**
 * Created by jixinwang on 2022/10/12
 */
public class MqApplierDumpEventActivity extends FetcherDumpEventActivity {

    @InstanceResource
    public MqPosition mqPosition;

    @Override
    protected FetcherSlaveServer getFetcherSlaveServer() {
        return new FetcherSlaveServer(config, new MqPooledConnector(config.getEndpoint()), new MqAbstractByteBufConverter());
    }

    @Override
    protected void persistPosition(GtidSet gtidSet) {
        mqPosition.union(gtidSet);
        logger.info("[Merge][{}] messenger init merge: {}", registryKey, gtidSet.toString());
    }

    @Override
    protected void persistPosition(Gtid gtid) {
        mqPosition.add(gtid);
    }
}
