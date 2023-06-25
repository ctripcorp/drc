package com.ctrip.framework.drc.replicator.container;

import com.ctrip.framework.drc.core.driver.config.GlobalConfig;
import com.ctrip.framework.drc.core.driver.config.InstanceStatus;
import com.ctrip.framework.drc.core.entity.Db;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorConfigDto;
import com.ctrip.framework.drc.core.server.container.ComponentRegistryHolder;
import com.ctrip.framework.drc.core.server.zookeeper.DrcZkConfig;
import com.ctrip.framework.drc.replicator.AbstractZkTest;
import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.cluster.ElectContext;
import com.ctrip.xpipe.foundation.DefaultFoundationService;
import com.ctrip.xpipe.zk.ZkClient;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.List;

import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.REPLICATOR_REGISTER_PATH;
import static com.ctrip.framework.drc.replicator.AllTests.*;

/**
 * @Author limingdong
 * @create 2020/4/18
 */
public class ReplicatorServerContainerTest extends AbstractZkTest {

    public static final String BU = "BBZ";

    public static final String MHA_NAME = "testOyMhaName";

    public static final String CLUSTER_NAME = "test_clusterName";

    public static final String UUID = "80ec424f-faeb-11e9-922d-fa163eb4df21";

    public static final Long APPID = 1222l;

    public static final int APPLIER_PORT = 8383;

    private ReplicatorConfig replicatorConfig;

    private ReplicatorConfigDto configDto = new ReplicatorConfigDto();

    @InjectMocks
    private ReplicatorServerContainer replicatorServerContainer = new ReplicatorServerContainer();

    @Mock
    private ZkClient zkClient;

    @Mock
    private ComponentRegistry registry;

    @Mock
    private DrcZkConfig drcZkConfig;

    @Mock
    private LeaderElectorManager leaderElectorManager;

    @Mock
    private LeaderElector leaderElector;

    @Mock
    private Exception exception;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        configDto.setBu(BU);
        configDto.setClusterAppId(APPID);
        configDto.setMhaName(MHA_NAME);
        configDto.setSrcDcName(System.getProperty(DefaultFoundationService.DATA_CENTER_KEY, GlobalConfig.DC));

        configDto.setStatus(InstanceStatus.ACTIVE.getStatus());
        configDto.setClusterName(CLUSTER_NAME);
        configDto.setGtidSet("");
        configDto.setApplierPort(APPLIER_PORT);

        List<String> uuids = Lists.newArrayList();
        Db db = new Db();
        db.setMaster(true);
        db.setPort(SRC_PORT);
        db.setIp(SRC_IP);
        db.setUuid(UUID);

        uuids.add(UUID);
        configDto.setReadUser(MYSQL_USER);
        configDto.setReadPassward(MYSQL_PASSWORD);
        configDto.setUuids(uuids);
        configDto.setPreviousMaster("");
        configDto.setMaster(db);

        replicatorConfig = configDto.toReplicatorConfig();

        when(drcZkConfig.getRegisterPath()).thenReturn(REPLICATOR_REGISTER_PATH);
        when(zkClient.get()).thenReturn(curatorFramework);

        ComponentRegistryHolder.initializeRegistry(registry);
        when(registry.add(anyObject())).thenReturn("");
    }

    @After
    public void tearDown() {
        if (curatorFramework != null) {
            curatorFramework.close();
        }
    }

    @Test
    public void testLeaderElectStop() throws Exception {
        when(leaderElectorManager.createLeaderElector(any(ElectContext.class))).thenReturn(leaderElector);
        replicatorServerContainer.getLeaderElector(getClass().getSimpleName());
        replicatorServerContainer.release();
        verify(leaderElector, times(1)).initialize();
        verify(leaderElector, times(1)).start();
        verify(leaderElector, times(1)).stop();
        verify(leaderElector, times(1)).dispose();
    }

}