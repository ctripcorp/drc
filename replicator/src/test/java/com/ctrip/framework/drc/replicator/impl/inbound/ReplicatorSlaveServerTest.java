package com.ctrip.framework.drc.replicator.impl.inbound;

import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.impl.ITransactionEvent;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidConfig;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidOperator;
import com.ctrip.framework.drc.replicator.impl.inbound.driver.ReplicatorPooledConnector;
import com.ctrip.framework.drc.replicator.impl.inbound.event.ReplicatorLogEventHandler;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.EventFilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.InboundFilterChainContext;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.EventTransactionCache;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.TransactionCache;
import com.ctrip.framework.drc.replicator.impl.monitor.DefaultMonitorManager;
import com.ctrip.framework.drc.replicator.store.EventStore;
import com.ctrip.framework.drc.replicator.store.FilePersistenceEventStore;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.UUID;

/**
 * Created by mingdongli
 * 2019/9/21 上午11:09.
 */
public class ReplicatorSlaveServerTest extends AbstractServerTest {

    private ReplicatorSlaveServer mySQLServer;

    private Endpoint endpoint;

    private MySQLSlaveConfig mySQLSlaveConfig;

    private EventStore eventStore;

    private TransactionCache transactionCache;

    private DefaultMonitorManager delayMonitor = new DefaultMonitorManager("ut");

    @Mock
    private Filter<ITransactionEvent> filterChain;

    @Mock
    private ReplicatorConfig replicatorConfig;

    @Mock
    private UuidOperator uuidOperator;

    @Mock
    private UuidConfig uuidConfig;

    private Set<UUID> uuids = Sets.newHashSet();

    @Before
    public void setUp() throws Exception {
        when(replicatorConfig.getWhiteUUID()).thenReturn(uuids);
        when(replicatorConfig.getRegistryKey()).thenReturn("");
        when(uuidOperator.getUuids(anyString())).thenReturn(uuidConfig);
        when(uuidConfig.getUuids()).thenReturn(Sets.newHashSet("c372080a-1804-11ea-8add-98039bbedf9c"));

        System.setProperty(SystemConfig.REPLICATOR_FILE_LIMIT, String.valueOf(1024 * 2));
        System.setProperty(SystemConfig.REPLICATOR_WHITE_LIST, String.valueOf(true));  //循环检测通过show variables like ""动态更新，集成测试使用该方式
        endpoint = new DefaultEndPoint(AbstractServerTest.IP, 8386, AbstractServerTest.USER, AbstractServerTest.PASSWORD);
        mySQLSlaveConfig = new MySQLSlaveConfig();
        mySQLSlaveConfig.setEndpoint(endpoint);
        mySQLSlaveConfig.setRegistryKey(AbstractServerTest.DESTINATION, MHA_NAME);
        mySQLServer = new ReplicatorSlaveServer(mySQLSlaveConfig, new ReplicatorPooledConnector(mySQLSlaveConfig.getEndpoint()), null);  //需要连接的master信息
        eventStore = new FilePersistenceEventStore(null, uuidOperator, replicatorConfig);
        transactionCache = new EventTransactionCache(eventStore, filterChain, "ut_test");
        LogEventHandler eventHandler = new ReplicatorLogEventHandler(transactionCache, delayMonitor, new EventFilterChainFactory().createFilterChain(new InboundFilterChainContext.Builder().build()));
        mySQLServer.setLogEventHandler(eventHandler);
    }

    @Test
    public void testSlaveId() {
        InetSocketAddress socketAddress = new InetSocketAddress("10.60.44.132", 55944);
        byte[] addr = socketAddress.getAddress().getAddress();
        String destination = "bbztriptrackShardBaseDB_dalcluster";
        int salt = (destination != null) ? destination.hashCode() : 0;
        Assert.assertEquals( ((0x7f & salt) << 24) + ((0xff & (int) addr[1]) << 16) // NL
                + ((0xff & (int) addr[2]) << 8) // NL
                + (0xff & (int) addr[3]) , 1765551236);
    }

    @Test
    public void testStart() throws Exception {
        eventStore.initialize();
        mySQLServer.initialize();
        mySQLServer.start();
        Thread.currentThread().join();
    }

}
