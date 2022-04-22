package com.ctrip.framework.drc.replicator.container.zookeeper;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.zk.ZkClient;
import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.REPLICATOR_UUIDS_PATH;

/**
 * @Author limingdong
 * @create 2022/4/2
 */
@Component
public class DefaultUuidOperator implements UuidOperator {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ZkClient zkClient;

    @Override
    public UuidConfig getUuids(String key) {
        CuratorFramework curatorFramework = zkClient.get();
        try {
            String registerPath = REPLICATOR_UUIDS_PATH + "/" + key;
            if (!checkPath(curatorFramework, registerPath)) {
                return new UuidConfig(Sets.newHashSet());
            }
            byte[] uuidArray = curatorFramework.getData().forPath(REPLICATOR_UUIDS_PATH + "/" + key);
            return Codec.DEFAULT.decode(uuidArray, UuidConfig.class);
        } catch (Exception e) {
            logger.error("getUuids error for {}", key, e);
        }
        return new UuidConfig(Sets.newHashSet());
    }

    @Override
    public void setUuids(String key, UuidConfig config) {
        CuratorFramework curatorFramework = zkClient.get();
        try {
            String registerPath = REPLICATOR_UUIDS_PATH + "/" + key;
            checkPath(curatorFramework, registerPath);
            curatorFramework.inTransaction().check().forPath(registerPath).and().setData().forPath(registerPath, Codec.DEFAULT.encodeAsBytes(config)).and().commit();
        } catch (Exception e) {
            logger.error("setUuids error for {}", key, e);
        }
    }

    private boolean checkPath(CuratorFramework curatorFramework, String registerPath) throws Exception {
        if (curatorFramework.checkExists().forPath(registerPath) == null) {
            curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(registerPath);
            return false;
        }
        return true;
    }

    @VisibleForTesting
    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }
}
