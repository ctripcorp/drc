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
import org.assertj.core.util.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.Set;

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

        if (isUsed(SRC_PORT)) {
            Thread.sleep(1200);
            verify(gtidManager, times(1)).getExecutedGtids();  //for reconnect
        } else {
            verify(gtidManager, times(0)).getExecutedGtids();  //mysql not start
        }
    }

    @Test
    public void testCombine() {
        String testUuid = "19aa3243-6fa8-11ec-8030-b8599f4ac53c";
        Set<String> uuids = Sets.newHashSet();
        uuids.add(testUuid);
        uuids.add("026aa718-6eac-11ec-9293-98039ba567ea");
        uuids.add("cdc1156a-6ead-11ec-b1ce-98039ba56ce6");
        uuids.add("2e44ce1c-b1e1-11ec-a0b1-98039ba567ea");
        uuids.add("b5f22724-b1ce-11ec-81b7-98039ba567ea");

        when(gtidManager.getUuids()).thenReturn(uuids);
        when(gtidManager.removeUuid(testUuid)).thenReturn(true);
        GtidSet newgtidSet = new GtidSet("026aa718-6eac-11ec-9293-98039ba567ea:1-63397550,19aa3243-6fa8-11ec-8030-b8599f4ac53c:1-5065,2e44ce1c-b1e1-11ec-a0b1-98039ba567ea:1-45,76bc9dc6-3003-11e9-9bf2-1c98ec274fd4:1-4286772491,7907de4a-7eea-11ea-8764-48df3717a474:1-185948430,87b2b560-b51a-11ea-b2ee-1c34da5ad21c:1-515015,94022a0b-3009-11e9-9c19-1c98ec2831a8:1-201008,cdc1156a-6ead-11ec-b1ce-98039ba56ce6:1-10910,d619ddc6-30cc-11ec-9be3-b8599f4a900c:1-271949534");
        GtidSet oldgtidSet = new GtidSet("026aa718-6eac-11ec-9293-98039ba567ea:1-63397550,76bc9dc6-3003-11e9-9bf2-1c98ec274fd4:1-4286772491,7907de4a-7eea-11ea-8764-48df3717a474:1-185948430,87b2b560-b51a-11ea-b2ee-1c34da5ad21c:1-515015,94022a0b-3009-11e9-9c19-1c98ec2831a8:1-201008,d619ddc6-30cc-11ec-9be3-b8599f4a900c:1-271949534,e482173d-6eaf-11ec-9073-98039ba567ea:1-2041440,19aa3243-6fa8-11ec-8030-b8599f4ac53c:1-5064,cdc1156a-6ead-11ec-b1ce-98039ba56ce6:1-10910");
        GtidSet gtidSet = replicatorConnection.combine(newgtidSet, oldgtidSet);
        Assert.assertTrue(gtidSet.getUUIDs().size() == 9);
        GtidSet.UUIDSet uuidSet = gtidSet.getUUIDSet(testUuid);
        Assert.assertTrue(uuidSet.toString().equals(testUuid + ":1-5065"));
    }

}