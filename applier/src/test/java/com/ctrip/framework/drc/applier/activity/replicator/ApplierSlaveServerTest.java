package com.ctrip.framework.drc.applier.activity.replicator;

import com.ctrip.framework.drc.applier.activity.replicator.converter.ApplierByteBufConverter;
import com.ctrip.framework.drc.applier.activity.replicator.driver.ApplierPooledConnector;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.fetcher.activity.replicator.FetcherSlaveServer;
import com.ctrip.framework.drc.fetcher.activity.replicator.config.FetcherSlaveConfig;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by mingdongli
 * 2019/9/23 下午11:42.
 */
public class ApplierSlaveServerTest {

    private FetcherSlaveServer mySQLServer;

    private Endpoint endpoint;

    private FetcherSlaveConfig mySQLSlaveConfig;

    @Before
    public void setUp() throws Exception {
        endpoint = new DefaultEndPoint("10.2.87.120", 8383);
        mySQLSlaveConfig = new FetcherSlaveConfig();
        mySQLSlaveConfig.setEndpoint(endpoint);
        mySQLSlaveConfig.setRegistryKey(SystemConfig.INTEGRITY_TEST);
        mySQLSlaveConfig.setGtidSet(new GtidSet("9f51357e-1cb1-11e8-a7f9-fa163e00d7c8:1-131075293"));
//        mySQLSlaveConfig.setGtidSet(new GtidSet("9f51357e-1cb1-11e8-a7f9-fa163e00d7c8:1-108240921"));
        mySQLServer = new FetcherSlaveServer(mySQLSlaveConfig, new ApplierPooledConnector(endpoint), new ApplierByteBufConverter());
        mySQLSlaveConfig.setApplierName("4566");
        mySQLServer.setLogEventHandler((logEvent, logEventCallBack, exception) -> {
            
        });
    }

    @Test
    public void testStart() throws Exception {
        mySQLServer.initialize();
        mySQLServer.start();
        Thread.currentThread().join();
    }
}
