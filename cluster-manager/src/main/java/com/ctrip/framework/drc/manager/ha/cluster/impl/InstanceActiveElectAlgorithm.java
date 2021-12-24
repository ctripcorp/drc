package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.Instance;

import java.util.List;

/**
 * @Author limingdong
 * @create 2020/4/29
 */
public interface InstanceActiveElectAlgorithm<T extends Instance> {

    T select(String clusterId, List<T> toBeSelected);

}
