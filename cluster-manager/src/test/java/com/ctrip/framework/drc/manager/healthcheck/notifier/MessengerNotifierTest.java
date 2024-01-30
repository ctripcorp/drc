package com.ctrip.framework.drc.manager.healthcheck.notifier;

import com.ctrip.framework.drc.core.server.config.ApplierRegistryKey;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.ctrip.framework.drc.manager.AllTests.*;

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
    public void getBody() {
        List<String> domains = messengerNotifier.getDomains(dbCluster);
        for (String ipAndPort : domains) {
            Object object = messengerNotifier.getBody(ipAndPort, dbCluster, false);
            ApplierConfigDto config = (ApplierConfigDto) object;
            Assert.assertEquals(config.getApplyMode(), ApplyMode.mq.getType());
            Assert.assertEquals(config.getReplicator().mhaName, SystemConfig.DRC_MQ);
            Assert.assertEquals(config.getTarget().mhaName, OY_MHA_NAME);
            Assert.assertEquals(config.getRegistryKey(), ApplierRegistryKey.from(OY_MHA_NAME, DAL_CLUSTER_NAME, SystemConfig.DRC_MQ));
            Assert.assertEquals(propertiesJson,config.getProperties());

        }
    }
    @Test
    public void testGetDelayMonitorRegex(){
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
