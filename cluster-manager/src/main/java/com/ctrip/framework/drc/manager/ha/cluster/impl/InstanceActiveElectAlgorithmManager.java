package com.ctrip.framework.drc.manager.ha.cluster.impl;

/**
 * @Author limingdong
 * @create 2020/4/29
 */
public interface InstanceActiveElectAlgorithmManager {

    InstanceActiveElectAlgorithm get(String clusterId);
}
