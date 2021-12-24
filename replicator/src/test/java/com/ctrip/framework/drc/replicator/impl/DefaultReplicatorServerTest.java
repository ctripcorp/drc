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
//        System.setProperty(SystemConfig.REPLICATOR_FILE_LIMIT, String.valueOf(1024 * 2));
        System.setProperty(SystemConfig.REPLICATOR_WHITE_LIST, String.valueOf(true));  //循环检测通过show variables like ""动态更新，集成测试使用该方式
        replicatorConfig = new ReplicatorConfig();

        boolean master = true;
        if (master) {
            replicatorConfig.setStatus(InstanceStatus.ACTIVE.getStatus());
            endpoint = new DefaultEndPoint("10.2.83.109", 3306, "root", "root");
            replicatorConfig.setApplierPort(REPLICATOR_MASTER_PORT + 3);
            replicatorConfig.setRegistryKey("test", "consume");
        } else {
            replicatorConfig.setStatus(InstanceStatus.INACTIVE.getStatus());
            endpoint = new DefaultEndPoint(AbstractServerTest.IP, 8383, AbstractServerTest.USER, AbstractServerTest.PASSWORD);
            replicatorConfig.setApplierPort(REPLICATOR_MASTER_PORT + 7);
            replicatorConfig.setRegistryKey("test", "mhaNamebackup");

        }
//        endpoint = new DefaultEndPoint("10.2.83.109", 3306, "root", "root");
//        endpoint = new DefaultEndPoint("10.5.20.243", 55111, "m_drc_r", "5hf1ikIdfox+kddvkdgQ");

//        endpoint = new DefaultEndPoint(AbstractServerTest.IP, AbstractServerTest.PORT, AbstractServerTest.USER, AbstractServerTest.PASSWORD);


        replicatorConfig.setEndpoint(endpoint);
        replicatorConfig.setWhiteUUID(Sets.newHashSet(UUID.fromString("12af97a8-2f0f-11eb-b4a7-d6dccc6aae29")));
//        replicatorConfig.setRegistryKey(DESTINATION);

//        replicatorConfig.setGtidSet(new GtidSet("9f51357e-1cb1-11e8-a7f9-fa163e00d7c8:1-120581982"));
//        replicatorConfig.setGtidSet(new GtidSet("9f51357e-1cb1-11e8-a7f9-fa163e00d7c8:1-120411217:120411220-120411243:120411246-120539561:120539564-120564654:120564656-120564656:120564658-120569898:120569900-120569910:120569912-120569926:120569928-120569929:120569931-120569946:120569948-120569950:120569953-120569962:120569967-120569972:120569974-120569985:120569987-120569993:120569995-120569995:120569998-120569998:120570001-120570009:120570011-120570027:120570029-120570030:120570032-120570040:120570042-120570045:120570047-120570051:120570053-120581984"));
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
