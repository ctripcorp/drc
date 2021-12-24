package com.ctrip.framework.drc.manager.healthcheck;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.framework.drc.manager.zookeeper.AbstractDbClusterTest;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;

/**
 * @Author limingdong
 * @create 2020/5/19
 */
public class DefaultMySQLMasterManagerTest extends AbstractDbClusterTest {

    @InjectMocks
    private DefaultMySQLMasterManager mySQLMasterManager;

    @Mock
    private HeartBeat heartBeat;

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mySQLMasterManager.initialize();
        mySQLMasterManager.start();
    }

    @After
    public void tearDown() {
        super.tearDown();
        try {
            mySQLMasterManager.stop();
            mySQLMasterManager.dispose();
        } catch (Exception e) {
        }
    }

    @Test
    public void handleClusterAdd() throws InterruptedException {
        String clusterId = dbCluster.getId();
        doNothing().when(heartBeat).addServer(any(Endpoint.class));
        doNothing().when(currentMetaManager).setMySQLMaster(anyString(), any(Endpoint.class));

        String mysqlIp = LOCAL_IP;
        int mysqlPort = 13306;
        String passward = "root";
        String user = "root";
        DefaultEndPoint master = new DefaultEndPoint(mysqlIp, mysqlPort, user, passward);
        when(currentMetaManager.getMySQLMaster(clusterId)).thenReturn(master);

        mySQLMasterManager.handleClusterAdd(dbCluster);

        DbCluster dc;
        int RETRY_COUNT = 10;
        int retryTime = 0;
        do {
            dc = mySQLMasterManager.getDbs(clusterId);
            if (dc == null) {
                retryTime++;
                Thread.sleep(100);
            }
        } while (dc == null && retryTime <= RETRY_COUNT);

        Assert.assertNotNull(dc);

        DbCluster dbClusterClone = new DbCluster(clusterId);
        dbClusterClone.setName(dbCluster.getName());
        dbClusterClone.setDbs(new Dbs());
        for (Replicator r : dbCluster.getReplicators()) {
            dbClusterClone.addReplicator(r);
        }
        for (Applier a : dbCluster.getAppliers()) {
            dbClusterClone.addApplier(a);
        }
        Dbs dbs = dbCluster.getDbs();
        Dbs dbsClone = dbClusterClone.getDbs();
        dbsClone.setPreviousMaster(dbs.getPreviousMaster());
        dbsClone.setMonitorPassword(dbs.getMonitorPassword());
        dbsClone.setMonitorUser(dbs.getMonitorUser());
        dbsClone.setMonitorPassword(dbs.getMonitorPassword());
        dbsClone.setReadPassword(dbs.getReadPassword());
        dbsClone.setReadUser(dbs.getReadUser());
        dbsClone.setWriteUser(dbs.getWriteUser());
        dbsClone.setWritePassword(dbs.getWritePassword());
        for (Db db : dbs.getDbs()) {
            Db dbClone = new Db();
            dbClone.setIp(db.getIp());
            dbClone.setPort(db.getPort());
            dbClone.setMaster(!db.isMaster());
            dbClone.setUuid(db.getUuid());
            dbsClone.addDb(dbClone);
        }

        doNothing().when(currentMetaManager).switchMySQLMaster(anyString(), any(Endpoint.class));
        ClusterComparator clusterComparator = new ClusterComparator(dbCluster, dbClusterClone);
        clusterComparator.compare();
        mySQLMasterManager.handleClusterModified(clusterComparator);
        Mockito.verify(currentMetaManager, times(1)).switchMySQLMaster(anyString(), any(Endpoint.class));
    }
}