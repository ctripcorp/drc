package com.ctrip.framework.drc.manager.healthcheck.service.task;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;

/**
 * @Author limingdong
 * @create 2020/3/4
 */
public class MasterHeartbeatTaskTest {

    private ExecutorService dbClusterCheckExecutorService = ThreadUtils.newCachedThreadPool("MySQLHeartBeatImpl-Check");

    private Endpoint endpoint = new DefaultEndPoint("10.2.83.109", 3306, "root", "root");

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void doQuery() throws InterruptedException {
        while (true) {
            dbClusterCheckExecutorService.submit(new MasterHeartbeatTask(endpoint));
            Thread.sleep(5000);
        }
    }
}