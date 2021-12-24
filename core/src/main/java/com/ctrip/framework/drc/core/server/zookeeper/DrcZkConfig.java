package com.ctrip.framework.drc.core.server.zookeeper;


import com.ctrip.xpipe.zk.ZkConfig;

/**
 * Created by mingdongli
 * 2019/10/31 上午12:43.
 */
public interface DrcZkConfig extends ZkConfig {

    String getRegisterPath();

    String getZkConnectionString();
}
