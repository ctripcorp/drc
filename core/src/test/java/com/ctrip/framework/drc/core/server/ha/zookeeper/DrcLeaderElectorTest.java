package com.ctrip.framework.drc.core.server.ha.zookeeper;

import com.ctrip.xpipe.cluster.ElectContext;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.AllTests.ZK_PORT;

/**
 * @Author limingdong
 * @create 2020/4/1
 */
public class DrcLeaderElectorTest {

    protected CuratorFramework curatorFramework;

    protected CuratorFramework curatorFramework1;

    @Before
    public void setUp() throws Exception {
        if (curatorFramework == null) {
            curatorFramework = CuratorFrameworkFactory.newClient("127.0.0.1:" + ZK_PORT, new ExponentialBackoffRetry(1000, 3));
            curatorFramework.start();
        }

        if (curatorFramework1 == null) {
            curatorFramework1 = CuratorFrameworkFactory.newClient("127.0.0.1:" + ZK_PORT, new ExponentialBackoffRetry(1000, 3));
            curatorFramework1.start();
        }
    }

    @After
    public void tearDown() {
        curatorFramework.close();
        curatorFramework1.close();
    }

    @Test
    public void elect() throws Exception {

        String zkPath = "/drc/leader";
        CountDownLatch countDownLatch = new CountDownLatch(2);

        new Task(zkPath, "IP1", countDownLatch, curatorFramework).start();
        new Task(zkPath, "IP2", countDownLatch, curatorFramework1).start();

        countDownLatch.await(500, TimeUnit.MILLISECONDS);
    }

    class Task extends Thread {
        String leaderElectionZKPath;
        String leaderElectionID;
        CountDownLatch countDownLatch;
        CuratorFramework curator;

        public Task(String leaderElectionZKPath, String leaderElectionID, CountDownLatch countDownLatch, CuratorFramework curatorFramework) {
            this.leaderElectionZKPath = leaderElectionZKPath;
            this.leaderElectionID = leaderElectionID;
            this.countDownLatch = countDownLatch;
            this.curator = curatorFramework;
        }

        @Override
        public void run() {
            ElectContext electContext = new ElectContext(leaderElectionZKPath, leaderElectionID);
            DrcLeaderElector leaderElector = new DrcLeaderElector(electContext, curator);
            try {
                leaderElector.initialize();
                leaderElector.start();
                Assert.assertNotNull(curator.checkExists().forPath(leaderElectionZKPath + "/" + leaderElectionID));
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                countDownLatch.countDown();
            }
        }
    }
}