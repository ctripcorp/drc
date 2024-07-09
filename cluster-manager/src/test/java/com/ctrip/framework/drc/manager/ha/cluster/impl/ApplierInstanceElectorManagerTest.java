package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.manager.ha.meta.RegionCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;

/**
 * @author: yongnian
 * @create: 2024/7/9 10:28
 */
public class ApplierInstanceElectorManagerTest {

    @Mock
    RegionCache regionCache;

    @InjectMocks
    ApplierInstanceElectorManager applierInstanceElectorManager;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGetApplier() {
        String clusterId = "clusterId";
        DbCluster dbCluster = new DbCluster(clusterId)
                .addApplier(new Applier().setIp("db_applier_ip_1").setPort(8080).setTargetMhaName("mha1").setIncludedDbs("db1"))
                .addApplier(new Applier().setIp("db_applier_ip_2").setPort(8080).setTargetMhaName("mha1").setIncludedDbs("db1"));
        when(regionCache.getCluster(clusterId)).thenReturn(dbCluster);

        Assert.assertNull(applierInstanceElectorManager.getApplier(clusterId, "db_applier_ip_1", 8080, "mha1", null));
        Assert.assertNull(applierInstanceElectorManager.getApplier(clusterId, "db_applier_ip_1", 8080, "mha1", "db2"));
        Assert.assertNull(applierInstanceElectorManager.getApplier(clusterId, "db_applier_ip_1", 8080, "mha2", "db1"));
        Assert.assertNull(applierInstanceElectorManager.getApplier(clusterId, "db_applier_ip_1", 8080, "mha2", "db2"));
        Assert.assertNull(applierInstanceElectorManager.getApplier(clusterId, "db_applier_ip_2", 8080, "mha1", "db2"));
        Assert.assertNotNull(applierInstanceElectorManager.getApplier(clusterId, "db_applier_ip_1", 8080, "mha1", "db1"));

        dbCluster = new DbCluster(clusterId)
                .addApplier(new Applier().setIp("db_applier_ip_1").setPort(8080).setTargetMhaName("mha1").setIncludedDbs(null))
                .addApplier(new Applier().setIp("db_applier_ip_2").setPort(8080).setTargetMhaName("mha1").setIncludedDbs(null));
        when(regionCache.getCluster(clusterId)).thenReturn(dbCluster);

        Assert.assertNull(applierInstanceElectorManager.getApplier(clusterId, "db_applier_ip_1", 8080, "mha1", "db2"));
        Assert.assertNull(applierInstanceElectorManager.getApplier(clusterId, "db_applier_ip_1", 8080, "mha2", "db1"));
        Assert.assertNull(applierInstanceElectorManager.getApplier(clusterId, "db_applier_ip_1", 8080, "mha2", "db2"));
        Assert.assertNull(applierInstanceElectorManager.getApplier(clusterId, "db_applier_ip_2", 8080, "mha1", "db2"));
        Assert.assertNull(applierInstanceElectorManager.getApplier(clusterId, "db_applier_ip_3", 8080, "mha1", null));
        Assert.assertNotNull(applierInstanceElectorManager.getApplier(clusterId, "db_applier_ip_1", 8080, "mha1", null));


    }
}
