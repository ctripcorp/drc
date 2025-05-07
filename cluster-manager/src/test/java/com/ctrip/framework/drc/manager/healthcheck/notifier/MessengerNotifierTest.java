package com.ctrip.framework.drc.manager.healthcheck.notifier;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.server.config.ApplierRegistryKey;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerConfigDto;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.ctrip.framework.drc.manager.AllTests.DAL_CLUSTER_NAME;
import static com.ctrip.framework.drc.manager.AllTests.OY_MHA_NAME;

/**
 * Created by jixinwang on 2022/11/15
 */
public class MessengerNotifierTest extends AbstractNotifierTest {

    private MessengerNotifier messengerNotifier;

    String propertiesJson = "{\"nameFilter\":\"drc1\\\\..*\",\"dataMediaConfig\":{\"rowsFilters\":[]},\"mqConfigs\":[{\"table\":\"drc1\\\\..*\",\"topic\":\"fx.drc.drc1.test_mq\",\"processor\":null,\"mqType\":\"qmq\",\"serialization\":\"json\",\"persistent\":false,\"persistentDb\":null,\"order\":true,\"orderKey\":\"id\",\"delayTime\":0}]}";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        messengerNotifier = MessengerNotifier.getInstance();
    }

    @Test
    public void testGetSuffix() {
        DbCluster dbCluster1 = new DbCluster().addMessenger(new Messenger().setIp("10.10.10.12").setPort(8080).setApplyMode(ApplyMode.mq.getType()))
                .addMessenger(new Messenger().setIp("10.10.10.11").setPort(8080).setApplyMode(ApplyMode.mq.getType()));
        String suffix = messengerNotifier.getMessengerRegistryKeySuffix(dbCluster1);
        Assert.assertEquals("_drc_mq", suffix);

        dbCluster1 = new DbCluster().addMessenger(new Messenger().setIp("10.10.10.12").setPort(8080).setApplyMode(ApplyMode.kafka.getType()))
                .addMessenger(new Messenger().setIp("10.10.10.11").setPort(8080).setApplyMode(ApplyMode.kafka.getType()));
        suffix = messengerNotifier.getMessengerRegistryKeySuffix(dbCluster1);
        Assert.assertEquals("_drc_kafka", suffix);

        dbCluster1 = new DbCluster().addMessenger(new Messenger().setIp("10.10.10.12").setPort(8080).setApplyMode(ApplyMode.db_mq.getType()))
                .addMessenger(new Messenger().setIp("10.10.10.11").setPort(8080).setApplyMode(ApplyMode.db_mq.getType()));
        suffix = messengerNotifier.getMessengerRegistryKeySuffix(dbCluster1);
        Assert.assertEquals("_drc_mq", suffix);
    }

    @Test(expected = RuntimeException.class)
    public void testGetSuffixException() {
        DbCluster dbCluster1 = new DbCluster().addMessenger(new Messenger().setIp("10.10.10.12").setPort(8080).setApplyMode(ApplyMode.mq.getType()))
                .addMessenger(new Messenger().setIp("10.10.10.11").setPort(8080).setApplyMode(ApplyMode.kafka.getType()));
        messengerNotifier.getMessengerRegistryKeySuffix(dbCluster1);
        Assert.fail();
    }

    @Test(expected = RuntimeException.class)
    public void testGetSuffixException2() {
        DbCluster dbCluster1 = new DbCluster().addMessenger(new Messenger().setIp("10.10.10.12").setPort(8080).setApplyMode(ApplyMode.mq.getType()))
                .addMessenger(new Messenger().setIp("10.10.10.11").setPort(8080).setApplyMode(ApplyMode.db_mq.getType()));
        messengerNotifier.getMessengerRegistryKeySuffix(dbCluster1);
        Assert.fail();
    }

    @Test
    public void getBody() {
        List<String> domains = messengerNotifier.getDomains(dbCluster);
        Assert.assertFalse(domains.isEmpty());
        for (String ipAndPort : domains) {
            Object object = messengerNotifier.getBody(ipAndPort, dbCluster, false);
            MessengerConfigDto config = (MessengerConfigDto) object;

            Assert.assertEquals(config.getApplyMode(), ApplyMode.mq.getType());
            Assert.assertEquals(SystemConfig.DRC_MQ, config.getReplicator().mhaName);
            Assert.assertEquals(OY_MHA_NAME, config.getTarget().mhaName);

            String registryKey = ApplierRegistryKey.from(OY_MHA_NAME, DAL_CLUSTER_NAME, SystemConfig.DRC_MQ);
            Assert.assertEquals("integration-test.fxdrc._drc_mq", registryKey);
            Assert.assertEquals(config.getRegistryKey(), registryKey);

            Assert.assertEquals(propertiesJson, config.getProperties());

        }
    }

    @Test
    public void testGetDelayMonitorRegex() {
        //
        String db1 = messengerNotifier.getDelayMonitorRegex(ApplyMode.db_mq.getType(), "db1");
        Assert.assertEquals("drcmonitordb\\.(dly_db1)", db1);
        System.out.println(db1);
        //
        String db2 = messengerNotifier.getDelayMonitorRegex(ApplyMode.mq.getType(), null);
        Assert.assertEquals("drcmonitordb\\.(delaymonitor)", db2);
        System.out.println(db2);
    }
}
