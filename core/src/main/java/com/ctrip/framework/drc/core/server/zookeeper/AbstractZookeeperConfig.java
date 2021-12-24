package com.ctrip.framework.drc.core.server.zookeeper;

import org.apache.curator.framework.CuratorFramework;

/**
 * Created by mingdongli
 * 2019/11/3 上午9:29.
 */
public abstract class AbstractZookeeperConfig extends AbstractCoreConfig {

    @Override
    public String getRegisterPath() {
        return getPath();
    }

    @Override
    public int getZkConnectionTimeoutMillis() {
        return 0;
    }

    @Override
    public int getZkCloseWaitMillis() {
        return 0;
    }

    @Override
    public int getZkRetries() {
        return 0;
    }

    @Override
    public int getSleepMsBetweenRetries() {
        return 0;
    }

    @Override
    public int getZkSessionTimeoutMillis() {
        return 0;
    }

    @Override
    public int waitForZkConnectedMillis() {
        return 0;
    }

    @Override
    public CuratorFramework create(String address) throws InterruptedException {
        return null;
    }

    protected abstract String getPath();
}
