package com.ctrip.framework.drc.manager.ha.meta.server;

/**
 * @Author limingdong
 * @create 2020/5/17
 */
public interface ClusterManagerMultiDcServiceManager {

    ClusterManagerMultiDcService getOrCreate(String clusterManagerAddress);

}
