package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.Db;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dbs;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.times;

/**
 * Created by jixinwang on 2023/8/31
 */
public class ApplierInstanceManagerTest {

    @InjectMocks
    public ApplierInstanceManager applierInstanceManager;

    @Mock
    protected InstanceStateController instanceStateController;

    @Mock
    protected CurrentMetaManager currentMetaManager;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void handleClusterModified() {
        String clusterId = "test_id";

        Dbs dbs = new Dbs();
        Db db = new Db();
        dbs.addDb(db);

        DbCluster current = new DbCluster();
        current.setDbs(dbs);
        current.setId(clusterId);
        Applier applier = new Applier();
        applier.setIp("127.0.0.1");
        applier.setPort(8080);
        current.addApplier(applier);

        DbCluster future = new DbCluster();
        future.setDbs(dbs);
        future.setId(clusterId);
        Applier applier2 = new Applier();
        applier2.setProperties("test_property");
        applier2.setIp("127.0.0.1");
        applier2.setPort(8080);
        future.addApplier(applier2);

        ClusterComparator comparator = new ClusterComparator(current, future);
        comparator.compare();

        Mockito.when(currentMetaManager.getActiveApplier(Mockito.anyString(), Mockito.anyString())).thenReturn(applier);
        applierInstanceManager.handleClusterModified(comparator);

        Mockito.verify(instanceStateController, times(1)).applierPropertyChange(clusterId, applier2);
    }
}