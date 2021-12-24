package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.Instance;

import java.util.List;

/**
 * @Author limingdong
 * @create 2020/4/29
 */
public class DefaultInstanceActiveElectAlgorithm<T extends Instance> implements InstanceActiveElectAlgorithm<T> {

    @Override
    public T select(String clusterId, List<T> toBeSelected) {

        if(toBeSelected.size() > 0){
            T result = toBeSelected.get(0);
            result.setMaster(true);
            return result;
        }
        return null;
    }
}
