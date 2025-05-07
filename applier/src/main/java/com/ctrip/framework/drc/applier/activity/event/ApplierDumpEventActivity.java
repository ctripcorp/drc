package com.ctrip.framework.drc.applier.activity.event;

import com.ctrip.framework.drc.applier.activity.replicator.converter.ApplierByteBufConverter;
import com.ctrip.framework.drc.applier.activity.replicator.driver.ApplierPooledConnector;
import com.ctrip.framework.drc.fetcher.activity.event.FetcherDumpEventActivity;
import com.ctrip.framework.drc.core.driver.binlog.gtid.Gtid;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.fetcher.activity.replicator.FetcherSlaveServer;

/**
 * @Author limingdong
 * @create 2021/3/4
 */
public class ApplierDumpEventActivity extends FetcherDumpEventActivity {

    @Override
    protected FetcherSlaveServer getFetcherSlaveServer() {
        return new FetcherSlaveServer(config, new ApplierPooledConnector(config.getEndpoint()), new ApplierByteBufConverter());
    }

    protected void persistPosition(GtidSet gtidSet) {

    }

    protected void persistPosition(Gtid gtid) {

    }
}
