package com.ctrip.framework.drc.manager.ha.meta.server;

/**
 * @Author limingdong
 * @create 2020/5/17
 */
public interface ClusterManagerMultiDcService extends ClusterManagerService{

    void upstreamChange(String clusterId, String backupClusterId, String ip, int applierPort);

}
