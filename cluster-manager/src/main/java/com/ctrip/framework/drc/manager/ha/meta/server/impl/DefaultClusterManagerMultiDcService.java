package com.ctrip.framework.drc.manager.ha.meta.server.impl;

import com.ctrip.framework.drc.manager.ha.cluster.META_SERVER_SERVICE;
import com.ctrip.framework.drc.manager.ha.meta.server.ClusterManagerMultiDcService;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author limingdong
 * @create 2020/5/17
 */
public class DefaultClusterManagerMultiDcService extends AbstractMetaService implements ClusterManagerMultiDcService {

    private String  upstreamchangePath;

    private String  clusterManagerAddress;

    public DefaultClusterManagerMultiDcService(String clusterManagerAddress) {
        this(clusterManagerAddress, DEFAULT_RETRY_TIMES, DEFAULT_RETRY_INTERVAL_MILLI);
    }

    public DefaultClusterManagerMultiDcService(String clusterManagerAddress, int retryTimes, int retryIntervalMilli) {
        super(retryTimes, retryIntervalMilli);
        this.clusterManagerAddress = clusterManagerAddress;
        upstreamchangePath = META_SERVER_SERVICE.UPSTREAM_CHANGE.getRealPath(clusterManagerAddress);
    }

    @Override
    public void upstreamChange(String clusterId, String backupClusterId, String ip, int port) {

        restTemplate.put(upstreamchangePath, null, clusterId, backupClusterId, ip, port);
    }

    @Override
    protected List<String> getMetaServerList() {

        List<String> result = new ArrayList<>();
        result.add(clusterManagerAddress);
        return result;
    }

    @Override
    public String toString() {

        return String.format("%s[%s]", getClass().getSimpleName(), clusterManagerAddress);
    }

}
