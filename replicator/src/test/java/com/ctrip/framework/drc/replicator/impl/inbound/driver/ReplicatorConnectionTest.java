package com.ctrip.framework.drc.replicator.impl.inbound.driver;

import com.ctrip.framework.drc.core.driver.MySQLConnector;
import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.gtid.position.EntryPosition;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.command.packet.server.ResultSetPacket;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.ctrip.framework.drc.core.exception.dump.NetworkException;
import com.ctrip.framework.drc.replicator.MockTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.BorrowObjectException;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import static com.ctrip.framework.drc.replicator.AllTests.*;

/**
 * @Author limingdong
 * @create 2020/6/28
 */
public class ReplicatorConnectionTest extends MockTest {

    @InjectMocks
    private MockReplicatorConnection replicatorConnection;

    @Mock
    private LogEventHandler eventHandler;

    private MySQLConnector replicatorPooledConnector;

    @Mock
    private GtidManager gtidManager;

    @Mock
    private SchemaManager schemaManager;

    @Mock
    private SimpleObjectPool<NettyClient> simpleObjectPool;

    @Mock
    private NettyClient nettyClient;

    @Mock
    private static ResultSetPacket resultSetPacket;

    private MySQLSlaveConfig mySQLSlaveConfig;

    private static final int UPDATE_COMMAND_COUNT = 7;

    @Before
    public void setUp() throws Exception {
        Endpoint endpoint = new DefaultEndPoint(SRC_IP, SRC_PORT, MYSQL_USER, MYSQL_PASSWORD);
        replicatorPooledConnector = new ReplicatorPooledConnector(endpoint);

        mySQLSlaveConfig = new MySQLSlaveConfig();
        mySQLSlaveConfig.setEndpoint(endpoint);
        mySQLSlaveConfig.setGtidSet(new GtidSet(""));
        replicatorConnection = new MockReplicatorConnection(mySQLSlaveConfig, eventHandler, replicatorPooledConnector, gtidManager, schemaManager);

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
        String currentUuid = "026aa718-6eac-11ec-9293-98039ba567ea";
        replicatorConnection.setCurrentUuid(currentUuid);
        String testUuid = "19aa3243-6fa8-11ec-8030-b8599f4ac53c";
        GtidSet newgtidSet = new GtidSet("19aa3243-6fa8-11ec-8030-b8599f4ac53c:1-271949534");
        GtidSet oldgtidSet = new GtidSet("026aa718-6eac-11ec-9293-98039ba567ea:1-63397550");
        GtidSet gtidSet = replicatorConnection.combine(newgtidSet, oldgtidSet);
        Assert.assertTrue(gtidSet.getUUIDs().size() == 2);
        GtidSet.UUIDSet uuidSet = gtidSet.getUUIDSet(testUuid);
        Assert.assertTrue(uuidSet.toString().equals(testUuid + ":1-271949534"));
    }

    @Test
    public void testPurgedGtidset() throws BorrowObjectException {
        String gtid = "19aa3243-6fa8-11ec-8030-b8599f4ac53c:1-271949534";
        when(simpleObjectPool.borrowObject()).thenReturn(nettyClient);
        when(resultSetPacket.getFieldValues()).thenReturn(Lists.newArrayList("purged_gtid", gtid));
        String res = replicatorConnection.fetchPurgedGtidSet(simpleObjectPool);
        Assert.assertEquals(gtid, res);
    }

    @Test
    public void testFetchExecutedGtidSet() throws BorrowObjectException {
        String gtid = "19aa3243-6fa8-11ec-8030-b8599f4ac53c:1-271949534";
        when(simpleObjectPool.borrowObject()).thenReturn(nettyClient);
        when(resultSetPacket.getFieldValues()).thenReturn(Lists.newArrayList("", "1", "", "",gtid));
        EntryPosition entryPosition = replicatorConnection.fetchExecutedGtidSet(simpleObjectPool);
        Assert.assertEquals(gtid, entryPosition.getGtid());
    }

    @Test
    public void testFetchServerUuid() throws BorrowObjectException {
        String uuid = "19aa3243-6fa8-11ec-8030-b8599f4ac53c";
        when(simpleObjectPool.borrowObject()).thenReturn(nettyClient);
        when(resultSetPacket.getFieldValues()).thenReturn(Lists.newArrayList("server_uuid", uuid));
        String res = replicatorConnection.fetchServerUuid(simpleObjectPool);
        Assert.assertEquals(uuid, res);
    }

    @Test
    public void testRegisterToSlave() throws BorrowObjectException {
        when(simpleObjectPool.borrowObject()).thenReturn(nettyClient);
        replicatorConnection.registerToSlave(simpleObjectPool);
        verify(simpleObjectPool, times(1)).borrowObject();
    }

    @Test
    public void testEnvSettingAndRegister() throws BorrowObjectException {
        when(simpleObjectPool.borrowObject()).thenReturn(nettyClient);
        replicatorConnection.envSettingAndRegister(simpleObjectPool);
        verify(simpleObjectPool, times(1 + UPDATE_COMMAND_COUNT)).borrowObject();
    }

    @Test
    public void testUpdateSettings() throws BorrowObjectException {
        when(simpleObjectPool.borrowObject()).thenReturn(nettyClient);
        replicatorConnection.updateSettings(simpleObjectPool);  // 7 commands in UpdateCommandExecutor
        verify(simpleObjectPool, times(UPDATE_COMMAND_COUNT)).borrowObject();
    }

    static class MockReplicatorConnection extends ReplicatorConnection {

        public MockReplicatorConnection(MySQLSlaveConfig mySQLSlaveConfig, LogEventHandler eventHandler, MySQLConnector connector, GtidManager gtidManager, SchemaManager schemaManager) {
            super(mySQLSlaveConfig, eventHandler, connector, gtidManager, schemaManager);
        }

        @Override
        protected <ResultSetPacket> void handleFuture(CommandFuture<ResultSetPacket> commandFuture) {
            commandFuture.setSuccess((ResultSetPacket) resultSetPacket);
        }
    }
}