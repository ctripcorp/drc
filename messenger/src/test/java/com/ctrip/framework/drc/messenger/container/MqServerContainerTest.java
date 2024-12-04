package com.ctrip.framework.drc.messenger.container;

import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerConfigDto;
import com.ctrip.framework.drc.core.server.zookeeper.DrcZkConfig;
import com.ctrip.framework.drc.core.utils.SpringUtils;
import com.ctrip.framework.drc.messenger.mq.MqPositionResource;
import com.ctrip.framework.drc.messenger.server.MqServerInCluster;
import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.cluster.ElectContext;
import com.ctrip.xpipe.zk.impl.SpringZkClient;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;
import org.springframework.context.ApplicationContext;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * Created by shiruixin
 * 2024/11/8 16:38
 */
public class MqServerContainerTest {
    @InjectMocks
    private MqServerContainer applierServerContainer = new MqServerContainer();

    private String key = "cluster";

    @Mock
    private LeaderElectorManager leaderElectorManager;

    @Mock
    private LeaderElector mockLeaderElector;

    @Mock
    private DrcZkConfig drcZkConfig;

    MockedStatic<SpringUtils> mockSpringUtils;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

         mockSpringUtils = Mockito.mockStatic(SpringUtils.class);
        ApplicationContext mockContext = Mockito.mock(ApplicationContext.class);
        SpringZkClient zkClient = mock(SpringZkClient.class);
        mockSpringUtils.when(SpringUtils::getApplicationContext).thenReturn(mockContext);
        Mockito.when(mockContext.getBean(Mockito.eq(SpringZkClient.class))).thenReturn(zkClient);

    }

    @After
    public void tearDown(){
        mockSpringUtils.close();
    }

    //    @Test
    public void testSwitch() throws Exception {
        ConcurrentHashMap<String, MqServerInCluster> servers
                = new ConcurrentHashMap<>();
        MessengerConfigDto config1 = getMqConfigDto();
        servers.put(key, new MqServerInCluster(config1));
        assertTrue(servers.containsKey(key));

        boolean added = applierServerContainer.addServer(config1);
        Assert.assertTrue(added);
        added = applierServerContainer.addServer(config1);
        Assert.assertFalse(added);

        ApplierConfigDto config2 = new ApplierConfigDto();
        Lists.newArrayList(config2).forEach(config->{
            config.target = new DBInfo();
            config.target.ip = "127.0.0.1";
            config.target.uuid = "hello_mysql";
            config.target.password = "123456root";
            config.target.username = "root";
            config.replicator = new InstanceInfo();
            config.replicator.ip = "127.0.0.1";
            config.replicator.port = 8384;
        });

        added = applierServerContainer.addServer(config2);  //restart
        Assert.assertTrue(added);
    }

    @Test
    public void testRelease() throws Exception {
        MessengerConfigDto config = getMqConfigDto();
        MqServerInCluster applierServerInCluster = new MqServerInCluster(config);
        applierServerContainer.getServers().put("test_cluster", new MqServerInCluster(config));
        applierServerInCluster.initialize();
        applierServerInCluster.start();

        MqServerInCluster serverInCluster = (MqServerInCluster) applierServerContainer.getServers().values().iterator().next();
        MqPositionResource mqPositionResource = serverInCluster.getMqPositionResource();

        applierServerContainer.release();
        Assert.assertTrue(mqPositionResource.isDisposed());
    }

    @Test
    public void testAddServer() throws Exception {

        MessengerConfigDto config = getMqConfigDto();

        boolean added = applierServerContainer.addServer(config);
        Assert.assertTrue(added);
        added = applierServerContainer.addServer(config);
        Assert.assertFalse(added);

    }


    @Test
    public void testRegister() {
        Mockito.when(leaderElectorManager.createLeaderElector(Mockito.any(ElectContext.class))).thenReturn(mockLeaderElector);
        LeaderElector leaderElector = applierServerContainer.registerServer(key);
        Assert.assertNotNull(leaderElector);
        LeaderElector leaderElector1 = applierServerContainer.registerServer(key);
        Assert.assertEquals(leaderElector, leaderElector1);
    }

    private MessengerConfigDto getMqConfigDto() {
        MessengerConfigDto config1 = new MessengerConfigDto();
        Lists.newArrayList(config1).forEach(config->{
            config.target = new DBInfo();
            config.target.ip = "127.0.0.1";
            config.target.uuid = "hello_mysql_test";
            config.target.password = "123456rootxxx";
            config.target.username = "root";
            config.target.mhaName = "mha1";
            config.replicator = new InstanceInfo();
            config.replicator.ip = "127.0.0.1";
            config.replicator.port = 8383;
            config.replicator.mhaName = "mha2";
            config.cluster = "cluster";
            config.setApplyMode(ApplyMode.mq.getType());
        });

        return config1;
    }
}