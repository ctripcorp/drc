package com.ctrip.framework.drc.manager.ha.meta.server.impl;

import com.ctrip.framework.drc.manager.ha.meta.server.ClusterManagerMultiDcService;
import com.ctrip.framework.drc.manager.ha.meta.server.ClusterManagerMultiDcServiceManager;
import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.utils.MapUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author limingdong
 * @create 2020/5/17
 */
public class DefaultClusterManagerMultiDcServiceManager implements ClusterManagerMultiDcServiceManager {

    private Map<String, ClusterManagerMultiDcService> services = new ConcurrentHashMap<>();

    @Override
    public ClusterManagerMultiDcService getOrCreate(final String clusterManagerAddress) {

        return MapUtils.getOrCreate(services, clusterManagerAddress, new ObjectFactory<ClusterManagerMultiDcService>() {

            @Override
            public ClusterManagerMultiDcService create() {

                return new DefaultClusterManagerMultiDcService(clusterManagerAddress);
            }
        });
    }

}
