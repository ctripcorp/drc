package com.ctrip.framework.drc.replicator.impl;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.config.InstanceStatus;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.replicator.container.config.TableFilterConfiguration;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidConfig;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidOperator;
import com.ctrip.framework.drc.replicator.impl.inbound.AbstractServerTest;
import com.ctrip.framework.drc.replicator.impl.inbound.event.ReplicatorLogEventHandler;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.InboundLogEventContext;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.UuidFilter;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.SchemaManagerFactory;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.UUID;

/**
 * Created by mingdongli
 * 2019/9/23 下午3:59.
 */
public class DefaultReplicatorServerTest extends AbstractServerTest {

    private Endpoint endpoint;

    private DefaultReplicatorServer replicatorServer;

    private ReplicatorConfig replicatorConfig;

    public static final String UUID_1 = "12af97a8-2f0f-11eb-b4a7-d6dccc6aae21";

    public static final String UUID_2 = "12af97a8-2f0f-11eb-b4a7-d6dccc6aae22";

    public static final String UUID_3 = "12af97a8-2f0f-11eb-b4a7-d6dccc6aae23";

    @Before
    public void setUp() throws Exception {
        System.setProperty(SystemConfig.REPLICATOR_WHITE_LIST, String.valueOf(true));  //循环检测通过show variables like ""动态更新，集成测试使用该方式
        replicatorConfig = new ReplicatorConfig();

        boolean master = true;
        if (master) {
            replicatorConfig.setStatus(InstanceStatus.ACTIVE.getStatus());
            endpoint = new DefaultEndPoint(AbstractServerTest.IP, AbstractServerTest.PORT, AbstractServerTest.USER, AbstractServerTest.PASSWORD);
            replicatorConfig.setApplierPort(REPLICATOR_MASTER_PORT + 3);
            replicatorConfig.setRegistryKey("test", "consume");
        } else {
            replicatorConfig.setStatus(InstanceStatus.INACTIVE.getStatus());
            endpoint = new DefaultEndPoint(AbstractServerTest.IP, 8383, AbstractServerTest.USER, AbstractServerTest.PASSWORD);
            replicatorConfig.setApplierPort(REPLICATOR_MASTER_PORT + 7);
            replicatorConfig.setRegistryKey("test", "mhaNamebackup");

        }

        replicatorConfig.setEndpoint(endpoint);
        replicatorConfig.setWhiteUUID(Sets.newHashSet(UUID.fromString(UUID_3)));
        replicatorConfig.setGtidSet(new GtidSet(UUID_3 + ":1-7"));

        replicatorServer = new DefaultReplicatorServer(replicatorConfig, SchemaManagerFactory.getOrCreateMySQLSchemaManager(replicatorConfig), new TableFilterConfiguration(), new UuidOperator() {
            @Override
            public UuidConfig getUuids(String key) {
                return new UuidConfig(Sets.newHashSet(UUID_1, UUID_2));
            }

            @Override
            public void setUuids(String key, UuidConfig value) {

            }
        });

    }

    @Test
    public void testUUids() {
        ReplicatorLogEventHandler replicatorLogEventHandler = replicatorServer.getLogEventHandler();
        Filter<InboundLogEventContext> filter = replicatorLogEventHandler.getFilterChain();
        boolean pass = false;
        while (filter != null) {
            if (filter instanceof UuidFilter) {
                UuidFilter uuidFilter = (UuidFilter) filter;
                Set<UUID> uuidSet =  uuidFilter.getWhiteList();
                Assert.assertTrue(uuidSet.size() == 3);
                Assert.assertTrue(uuidSet.contains(UUID.fromString(UUID_1)));
                Assert.assertTrue(uuidSet.contains(UUID.fromString(UUID_2)));
                Assert.assertTrue(uuidSet.contains(UUID.fromString(UUID_3)));
                pass = true;
                break;
            }
            filter = filter.getSuccessor();
        }
        Assert.assertTrue(pass);
    }

//    @Test
    public void testStartMySQLMaster() throws Exception {
        replicatorServer.initialize();
        replicatorServer.start();
        Thread.currentThread().join();
    }

}
