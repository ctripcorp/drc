package com.ctrip.framework.drc.replicator.impl;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.config.InstanceStatus;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.replicator.container.config.TableFilterConfiguration;
import com.ctrip.framework.drc.replicator.impl.inbound.AbstractServerTest;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.SchemaManagerFactory;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

/**
 * Created by mingdongli
 * 2019/9/23 下午3:59.
 */
public class DefaultReplicatorServerTest extends AbstractServerTest {

    private Endpoint endpoint;

    private DefaultReplicatorServer replicatorServer;

    private ReplicatorConfig replicatorConfig;

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
        replicatorConfig.setWhiteUUID(Sets.newHashSet(UUID.fromString("12af97a8-2f0f-11eb-b4a7-d6dccc6aae29")));
        replicatorConfig.setGtidSet(new GtidSet("12af97a8-2f0f-11eb-b4a7-d6dccc6aae29:1-7"));

        replicatorServer = new DefaultReplicatorServer(replicatorConfig, SchemaManagerFactory.getOrCreateMySQLSchemaManager(replicatorConfig), new TableFilterConfiguration());

        replicatorServer.initialize();
    }

    @Test
    public void testStartMySQLMaster() throws Exception {
        replicatorServer.start();
        Thread.currentThread().join();
    }
}
