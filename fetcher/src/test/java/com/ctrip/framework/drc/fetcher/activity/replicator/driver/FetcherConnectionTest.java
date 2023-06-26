package com.ctrip.framework.drc.fetcher.activity.replicator.driver;

import com.ctrip.framework.drc.core.driver.MySQLConnector;
import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.converter.ByteBufConverter;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.exception.dump.NetworkException;
import com.ctrip.framework.drc.core.server.config.replicator.MySQLMasterConfig;
import com.ctrip.framework.drc.fetcher.activity.replicator.config.FetcherSlaveConfig;
import com.ctrip.framework.drc.fetcher.resource.context.NetworkContextResource;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;

/**
 * @Author limingdong
 * @create 2021/3/15
 */
public class FetcherConnectionTest {

    private static final String IP = "127.0.0.1";

    private static final int PORT = 3306;

    private FetcherConnection applierConnection;

    private FetcherSlaveConfig applierSlaveConfig = new FetcherSlaveConfig();

    @Mock
    private LogEventHandler eventHandler;

    @Mock
    private NetworkContextResource networkContextResource;

    @Mock
    private ByteBufConverter byteBufConverter;

    private Endpoint endpoint = new DefaultEndPoint(IP, PORT, "root", "123456");

    private MySQLConnector connector;

    private MySQLMasterConfig mySQLMasterConfig;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        connector = new FetcherPooledConnector(endpoint) {
            @Override
            public String getModuleName() {
                return "fetcher";
            }
        };

        applierSlaveConfig.setEndpoint(endpoint);
        applierSlaveConfig.setRegistryKey("ut_" + getClass().getSimpleName());
        applierSlaveConfig.setGtidSet(new GtidSet(""));
        applierSlaveConfig.setApplierName(applierSlaveConfig.getRegistryKey());

        applierConnection = new FetcherConnection(applierSlaveConfig, eventHandler, connector, networkContextResource, byteBufConverter);

        mySQLMasterConfig = new MySQLMasterConfig();
        mySQLMasterConfig.setIp(IP);
        mySQLMasterConfig.setPort(PORT + 5000);

    }

    @After
    public void tearDown() throws Exception {
        applierConnection.stop();
        applierConnection.dispose();
    }

    @Test
    public void testReconnect() throws Exception {
        ReplicatorServer replicatorServer = getReplicatorServer();
        when(networkContextResource.fetchGtidSet()).thenReturn(new GtidSet(""));

        applierConnection.initialize();
        applierConnection.start();

        applierConnection.dump(resultCode -> eventHandler.onLogEvent(null, null, new NetworkException(resultCode.getMessage())));
        Mockito.verify(networkContextResource, times(0)).fetchGtidSet();  //connected
        stopReplicatorServer(replicatorServer);
        Thread.sleep(1100);
        Mockito.verify(networkContextResource, atLeast(1)).queryTheNewestGtidset();  //no binlog, so no exception, stop server, reconnect

        replicatorServer = getReplicatorServer();

        Mockito.verify(networkContextResource, atLeast(1)).fetchGtidSet();
        stopReplicatorServer(replicatorServer);
    }

    private ReplicatorServer getReplicatorServer() throws Exception {
        ReplicatorServer replicatorServer = new ReplicatorServer(mySQLMasterConfig);
        replicatorServer.initialize();
        replicatorServer.start();
        return replicatorServer;
    }

    private void stopReplicatorServer(ReplicatorServer replicatorServer) throws Exception {
        replicatorServer.stop();
        replicatorServer.dispose();
    }
}
