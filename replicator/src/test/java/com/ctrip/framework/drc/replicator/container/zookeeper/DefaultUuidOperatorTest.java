package com.ctrip.framework.drc.replicator.container.zookeeper;

import com.ctrip.framework.drc.replicator.AbstractZkTest;
import com.ctrip.xpipe.zk.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.assertj.core.util.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

/**
 * @Author limingdong
 * @create 2022/4/2
 */
public class DefaultUuidOperatorTest extends AbstractZkTest {

    private DefaultUuidOperator uuidOperator = new DefaultUuidOperator();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        uuidOperator.setZkClient(new ZkClient() {
            @Override
            public CuratorFramework get() {
                return curatorFramework;
            }

            @Override
            public void setZkAddress(String zkAddress) {

            }

            @Override
            public String getZkAddress() {
                return zkString;
            }
        });
    }

    @Test
    public void testOperator() {
        String KEY = "test_uuid_operator";
        String UUID = "1q2w3e4r5t6y7u";
        Set<String> uuids = Sets.newHashSet();
        uuids.add(UUID);

        uuidOperator.setUuids(KEY, new UuidConfig(uuids));
        UuidConfig uuidConfig = uuidOperator.getUuids(KEY);

        Set<String> readUuids = uuidConfig.getUuids();
        Assert.assertEquals(uuids.size(), readUuids.size());
        Assert.assertTrue(readUuids.contains(UUID));
    }

}