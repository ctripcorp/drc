package com.ctrip.framework.drc.replicator.container.zookeeper;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.zk.ZkClient;
import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.REPLICATOR_UUIDS_PATH;

/**
 * @Author limingdong
 * @create 2022/4/2
 */
@Component
public class DefaultUuidOperator implements UuidOperator {

    @Autowired
    protected ZkClient zkClient;

    @Override
    public UuidConfig getUuids(String key) {
        CuratorFramework curatorFramework = zkClient.get();
        try {
            String registerPath = REPLICATOR_UUIDS_PATH + "/" + key;
            if (curatorFramework.checkExists().forPath(registerPath) == null) {
                curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(registerPath);
                return new UuidConfig(Sets.newHashSet());
            }
            byte[] uuidArray = curatorFramework.getData().forPath(REPLICATOR_UUIDS_PATH + "/" + key);
            return Codec.DEFAULT.decode(uuidArray, UuidConfig.class);
        } catch (Exception e) {
        }
        return new UuidConfig(Sets.newHashSet());
    }

    @Override
    public void setUuids(String key, UuidConfig config) {
        CuratorFramework curatorFramework = zkClient.get();
        try {
            String registerPath = REPLICATOR_UUIDS_PATH + "/" + key;
            curatorFramework.inTransaction().check().forPath(registerPath).and().setData().forPath(registerPath, Codec.DEFAULT.encodeAsBytes(config)).and().commit();
        } catch (Exception e) {
        }
    }
}
