package com.ctrip.framework.drc.applier.confirmed.curator;

import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.xpipe.api.codec.Codec;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetData {

    private Logger logger = LoggerFactory.getLogger(getClass());

    CuratorFramework zkClient;

    @Before
    public void setUp() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zkClient = CuratorFrameworkFactory.newClient("127.0.0.1:2181", retryPolicy);
        zkClient.start();
    }

    @After
    public void tearDown() throws Exception {
        zkClient.close();
        zkClient = null;
    }

    @Test
    public void create() throws Exception {
        zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/applier/test-1/schemas", "{}".getBytes());
    }

    @Test
    public void getData() throws Exception {
        byte[] data = zkClient.getData().forPath("/applier/test-1/schemas");
        logger.info(new String(data));
    }

    @Test
    public void updateData() throws Exception {
        zkClient.setData().forPath("/applier/test-1/schemas", Codec.DEFAULT.encode(new ApplierConfigDto()).getBytes());
    }

    @Test
    public void useCodec() {
        logger.info(Codec.DEFAULT.encode(new ApplierConfigDto()));
    }
}
