package com.ctrip.framework.drc.manager.zookeeper;

import com.ctrip.framework.drc.core.driver.config.InstanceStatus;
import com.ctrip.framework.drc.core.server.container.ZookeeperValue;
import com.ctrip.framework.drc.manager.MockTest;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Random;
import java.util.Set;

import static com.ctrip.framework.drc.manager.AllTests.DAL_CLUSTER_ID;

/**
 * Created by mingdongli
 * 2019/11/3 下午10:12.
 */
public abstract class AbstractZkTest extends MockTest {

    protected static final String CLUSTER_ID = DAL_CLUSTER_ID;

    protected static final String ADDED_ID = "1qaz2wsx";

    protected static final String IP = "12.12.12.12";

    protected static final String UUID = "1234567890";

    protected static final int PORT = 1212;

    protected CuratorFramework curatorFramework;

    protected ZookeeperValue zookeeperValue = new ZookeeperValue();

    protected PersistentNode persistentNode;

    public void setUp() throws Exception {
        super.setUp();
        if (curatorFramework == null) {
            curatorFramework = CuratorFrameworkFactory.newClient("127.0.0.1:12181", new ExponentialBackoffRetry(1000, 3));
            curatorFramework.start();
        }

        zookeeperValue.setStatus(InstanceStatus.ACTIVE.getStatus());
        zookeeperValue.setClusterName(CLUSTER_ID);
    }

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

    protected void rmr(String path) throws Exception {
        curatorFramework.delete().deletingChildrenIfNeeded().forPath(path);
    }

    public static int randomPort() {
        return randomPort(10000, 20000, null);
    }

    public static int randomPort(int min, int max, Set<Integer> different) {

        Random random = new Random();

        for (int i = min; i <= max; i++) {
            int port = min + random.nextInt(max - min + 1);
            if ((different == null || !different.contains(new Integer(port))) && isUsable(port)) {
                return port;
            }
        }

        throw new IllegalStateException(String.format("random port not found:(%d, %d)", min, max));
    }

    protected static boolean isUsable(int port) {

        try (ServerSocket s = new ServerSocket()) {
            s.bind(new InetSocketAddress(port));
            return true;
        } catch (IOException e) {
        }
        return false;
    }
}
