package com.ctrip.framework.drc.applier.container;

import com.ctrip.framework.drc.applier.resource.mysql.DataSourceResource;
import com.ctrip.framework.drc.fetcher.resource.thread.Executor;
import com.ctrip.framework.drc.applier.server.ApplierServerInCluster;
import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.core.server.zookeeper.DrcZkConfig;
import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.cluster.ElectContext;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertTrue;

/**
 * @Author Slight
 * Jan 10, 2020
 */
public class ApplierServerContainerTest {

    @InjectMocks
    private ApplierServerContainer applierServerContainer = new ApplierServerContainer();

    private String key = "cluster";

    @Mock
    private LeaderElectorManager leaderElectorManager;

    @Mock
    private LeaderElector mockLeaderElector;

    @Mock
    private DrcZkConfig drcZkConfig;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

//    @Test
    public void testSwitch() throws Exception {
        ConcurrentHashMap<String, ApplierServerInCluster> servers
                = new ConcurrentHashMap<>();
        ApplierConfigDto config1 = getApplierConfigDto();
        servers.put(key, new ApplierServerInCluster(config1));
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
        ApplierConfigDto config = getApplierConfigDto();
        ApplierServerInCluster applierServerInCluster = new ApplierServerInCluster(config);
        applierServerContainer.getServers().put("test_cluster", new ApplierServerInCluster(config));
        applierServerInCluster.initialize();
        applierServerInCluster.start();

        ApplierServerInCluster serverInCluster = (ApplierServerInCluster) applierServerContainer.getServers().values().iterator().next();
        DataSourceResource dataSourceResource = serverInCluster.getDataSourceResource();
        dataSourceResource.executor = new Executor() {
            @Override
            public void execute(Runnable runnable) {

            }
        };
        if (dataSourceResource.canInitialize()) {
            dataSourceResource.initialize();
        }
        applierServerContainer.release();
        Assert.assertTrue(dataSourceResource.isDisposed());
    }

    @Test
    public void testAddServer() throws Exception {
        ApplierConfigDto config = getApplierConfigDto();
        boolean added = applierServerContainer.addServer(config);
        Assert.assertTrue(added);
        added = applierServerContainer.addServer(config);
        Assert.assertFalse(added);

    }

    @Test
    public void testAddServerWithTransactionTable() throws Exception {
        ApplierConfigDto config = getApplierConfigDto();
        config.setCluster("cluster2");
        config.setApplyMode(1);
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

    private ApplierConfigDto getApplierConfigDto() {
        ApplierConfigDto config1 = new ApplierConfigDto();
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
        });

        return config1;
    }
}
