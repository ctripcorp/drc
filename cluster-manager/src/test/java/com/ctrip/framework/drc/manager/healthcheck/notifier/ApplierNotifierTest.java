package com.ctrip.framework.drc.manager.healthcheck.notifier;

import com.ctrip.framework.drc.core.server.config.ApplierRegistryKey;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.ctrip.framework.drc.manager.AllTests.*;

/**
 * @Author limingdong
 * @create 2020/4/15
 */
public class ApplierNotifierTest extends AbstractNotifierTest {

    private ApplierNotifier applierNotifier;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        applierNotifier = ApplierNotifier.getInstance();

    }

    @Test
    public void getBody() {
        List<String> domains = applierNotifier.getDomains(dbCluster);
        for (String ipAndPort : domains) {
            Object object = applierNotifier.getBody(ipAndPort, dbCluster, false);
            ApplierConfigDto config = (ApplierConfigDto) object;
            Assert.assertEquals(config.getTarget().getCluster(), DAL_CLUSTER_NAME);
            Assert.assertEquals(config.getReplicator().mhaName, RB_MHA_NAME);
            Assert.assertEquals(config.getTarget().mhaName, OY_MHA_NAME);
            Assert.assertEquals(config.getIncludedDbs(), "db1,db2");
            Assert.assertEquals(config.getRegistryKey(), ApplierRegistryKey.from(OY_MHA_NAME, DAL_CLUSTER_NAME, RB_MHA_NAME));
        }
    }

}