package com.ctrip.framework.drc.performance.impl.server;

import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by jixinwang on 2021/9/22
 */
public class JdbcTestServerTest {

    private JdbcTestServer jdbcTestServer;

    @Before
    public void before() throws Exception {
        ApplierConfigDto config = new ApplierConfigDto();
        config.setCluster("appliertest_dalcluster");
        InstanceInfo replicator = new InstanceInfo();
        replicator.setMhaName("srcMha");
        replicator.setCluster("appliertest_dalcluster");
        config.setReplicator(replicator);
        DBInfo target = new DBInfo();
        target.setUsername("root");
        target.setPassword("123456");
        target.setIp("127.0.0.1");
        target.setPort(3306);
        target.setMhaName("destMha");
        config.setTarget(target);
        jdbcTestServer = new JdbcTestServer(config);
        jdbcTestServer.setThreadNum(2);
        jdbcTestServer.setExecuteCount(10);
    }

    @Test
    public void test() throws Exception {
        jdbcTestServer.initialize();
        jdbcTestServer.start();
    }
}
