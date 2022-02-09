package com.ctrip.framework.drc.manager.zookeeper;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.framework.drc.manager.AllTests.*;

/**
 * @Author limingdong
 * @create 2020/5/12
 */
public abstract class AbstractDbClusterTest extends AbstractZkTest {

    protected static final Long SERVER_ID = 123456789L;

    protected Drc drc;

    protected Dc current;

    protected DbCluster dbCluster;

    protected Replicator newReplicator = new Replicator();

    protected Applier newApplier = new Applier();

    protected String LOCAL_IP = "127.0.0.1";

    protected int backupPort = 8080;

    protected ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("unit_test");

    protected Pair<String, Integer> applierMaster;

    protected Endpoint mysqlMaster;

    protected DefaultEndPoint switchedMySQLMaster = new DefaultEndPoint(LOCAL_IP, 13307, "root", "root");

    @Before
    public void setUp() throws Exception {
        super.setUp();
        drc = DefaultSaxParser.parse(DRC_XML);
        current = drc.getDcs().get(DC);
        dbCluster = current.getDbClusters().get(CLUSTER_ID);

        int APPLIER_PORT = 4321;
        newReplicator.setIp(LOCAL_IP);
        newReplicator.setPort(HTTP_PORT);
        newReplicator.setGtidSkip("1234:123");
        newReplicator.setApplierPort(APPLIER_PORT);
        newReplicator.setMaster(false);

        newApplier.setTargetMhaName("mockTargetMha_*&^%");
        newApplier.setTargetName("mockTargetName_1234");
        newApplier.setTargetIdc("mockTargetIdc_*&^%");
        newApplier.setMaster(true);
        newApplier.setIp(LOCAL_IP);
        newApplier.setPort(backupPort);

        applierMaster = new Pair<>(newReplicator.getIp(), newReplicator.getApplierPort());

        String mysqlIp = LOCAL_IP;
        int mysqlPort = 13307;
        String passward = "12qwaszx";
        String user = "123qweasdzxc";
        mysqlMaster = new DefaultEndPoint(mysqlIp, mysqlPort, user, passward);
    }

    public Drc getDrc() {
        return drc;
    }

    public Dc getDc(String dc) {
        return getDrc().getDcs().get(dc);
    }

    protected DbCluster getCluster(String dc, String clusterId) {

        Dc dcMeta = getDc(dc);
        if (dcMeta == null) {
            return null;
        }
        DbCluster clusterMeta = dcMeta.getDbClusters().get(clusterId);
        return clusterMeta;
    }

    @After
    public void tearDown() {
        if (curatorFramework != null) {
            curatorFramework.close();
        }
        if (persistentNode != null) {
            try {
                persistentNode.close();
            } catch (IOException e) {
            }
        }
    }
}
