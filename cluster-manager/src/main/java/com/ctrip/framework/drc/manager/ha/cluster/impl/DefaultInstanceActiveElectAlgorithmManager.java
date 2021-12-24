package com.ctrip.framework.drc.manager.ha.cluster.impl;

import org.springframework.stereotype.Component;

/**
 * @Author limingdong
 * @create 2020/4/29
 */
@Component
public class DefaultInstanceActiveElectAlgorithmManager implements InstanceActiveElectAlgorithmManager {

    @Override
    public InstanceActiveElectAlgorithm get(String clusterId) {
        return new DefaultInstanceActiveElectAlgorithm();
    }
}
