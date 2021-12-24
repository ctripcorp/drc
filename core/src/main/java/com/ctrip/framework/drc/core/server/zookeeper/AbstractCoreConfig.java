package com.ctrip.framework.drc.core.server.zookeeper;

import com.ctrip.xpipe.config.AbstractConfigBean;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by mingdongli
 * 2019/10/30 下午11:48.
 */
public abstract class AbstractCoreConfig extends AbstractConfigBean implements DrcZkConfig {

    public static String DEFAULT_ZK_NAMESPACE = "drc";

    public static String KEY_ZK_ADDRESS  = "zk.address";

    public static String KEY_ZK_NAMESPACE  = "zk.namespace";

    private AtomicReference<String> zkConnection = new AtomicReference<>();

    private AtomicReference<String> zkNameSpace = new AtomicReference<>();

    @Override
    public String getZkConnectionString() {

        return getProperty(KEY_ZK_ADDRESS, zkConnection.get() == null ? "127.0.0.1:2181" : zkConnection.get());
    }

    public void setZkConnectionString(String zkConnectionString) {
        this.zkConnection.set(zkConnectionString);
    }

    @Override
    public String getZkNamespace(){
        return getProperty(KEY_ZK_NAMESPACE, zkNameSpace.get() == null ? DEFAULT_ZK_NAMESPACE : zkNameSpace.get());
    }

    public void setZkNameSpace(String zkNameSpace) {
        this.zkNameSpace.set(zkNameSpace);
    }
}
