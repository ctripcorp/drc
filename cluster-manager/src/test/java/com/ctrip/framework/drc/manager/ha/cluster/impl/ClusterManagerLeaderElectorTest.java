package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.zookeeper.AbstractZkTest;
import com.ctrip.xpipe.api.cluster.LeaderAware;
import com.ctrip.xpipe.zk.ZkClient;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.context.ApplicationContext;

import java.util.List;
import java.util.Map;

/**
 * @Author limingdong
 * @create 2020/5/13
 */
public class ClusterManagerLeaderElectorTest extends AbstractZkTest {

    @InjectMocks
    private ClusterManagerLeaderElector clusterManagerLeaderElector = new ClusterManagerLeaderElector();

    @Mock
    private ZkClient zkClient;

    @Mock
    private ClusterManagerConfig config;

    @Mock
    private ApplicationContext applicationContext;

    @Mock
    private LeaderAware leaderAware;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        try {
            curatorFramework.delete().deletingChildrenIfNeeded().forPath(clusterManagerLeaderElector.getLeaderElectPath());
        } catch (Exception e) {
        }
        curatorFramework.createContainers(clusterManagerLeaderElector.getLeaderElectPath());
        when(zkClient.get()).thenReturn(curatorFramework);
        when(config.getClusterServerId()).thenReturn(String.valueOf("qwer"));
        clusterManagerLeaderElector.setApplicationContext(applicationContext);
        Map<String, LeaderAware> leaderAwareMap = Maps.newConcurrentMap();
        leaderAwareMap.put("key", leaderAware);
        when(applicationContext.getBeansOfType(LeaderAware.class)).thenReturn(leaderAwareMap);

        clusterManagerLeaderElector.initialize();
    }

    @Test
    public void getLeaderElectPath() throws Exception {
        clusterManagerLeaderElector.start();
        Thread.sleep(300);
        List<String> servers = clusterManagerLeaderElector.getAllServers();
        Assert.assertEquals(servers.size(), 1);

        Assert.assertTrue(clusterManagerLeaderElector.amILeader());
        verify(leaderAware, times(1)).isleader();

    }

    @After
    public void tearDown(){
        try {
            clusterManagerLeaderElector.stop();
            clusterManagerLeaderElector.dispose();
        } catch (Exception e) {
        }
    }

}