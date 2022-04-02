package com.ctrip.framework.drc.replicator.impl.inbound.driver;

import com.ctrip.framework.drc.core.driver.MySQLConnector;
import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.ctrip.framework.drc.core.exception.dump.NetworkException;
import com.ctrip.framework.drc.replicator.MockTest;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.replicator.AllTests.*;

/**
 * @Author limingdong
 * @create 2020/6/28
 */
public class ReplicatorConnectionTest extends MockTest {

    @InjectMocks
    private ReplicatorConnection replicatorConnection;

    @Mock
    private LogEventHandler eventHandler;

    private MySQLConnector replicatorPooledConnector;

    @Mock
    private GtidManager gtidManager;

    @Mock
    private SchemaManager schemaManager;

    private MySQLSlaveConfig mySQLSlaveConfig;

    @Before
    public void setUp() throws Exception {
        Endpoint endpoint = new DefaultEndPoint(SRC_IP, SRC_PORT, MYSQL_USER, MYSQL_PASSWORD);
        replicatorPooledConnector = new ReplicatorPooledConnector(endpoint);

        mySQLSlaveConfig = new MySQLSlaveConfig();
        mySQLSlaveConfig.setEndpoint(endpoint);
        mySQLSlaveConfig.setGtidSet(new GtidSet(""));
        replicatorConnection = new ReplicatorConnection(mySQLSlaveConfig, eventHandler, replicatorPooledConnector, gtidManager, schemaManager);

        replicatorConnection.initialize();
        replicatorConnection.start();
    }

    @After
    public void tearDown() throws Exception {
        replicatorConnection.stop();
        replicatorConnection.dispose();
    }

    @Test //RuntimeException: dump command error : #HY000Binary log is not open
    public void testDump() throws InterruptedException {

        replicatorConnection.dump(resultCode -> eventHandler.onLogEvent(null, null, new NetworkException(resultCode.getMessage())));

        TimeUnit.SECONDS.sleep(1000);

        if (isUsed(SRC_PORT)) {
            Thread.sleep(1200);
            verify(gtidManager, times(1)).getExecutedGtids();  //for reconnect
        } else {
            verify(gtidManager, times(0)).getExecutedGtids();  //mysql not start
        }
    }

}