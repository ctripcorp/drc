package com.ctrip.framework.drc.manager.ha.meta.server.impl;

import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.manager.ha.cluster.META_SERVER_SERVICE;
import com.ctrip.framework.drc.manager.ha.meta.server.ClusterManagerService;
import com.google.common.base.Function;

import java.util.List;

/**
 * @Author limingdong
 * @create 2020/5/17
 */
public abstract class AbstractMetaService extends AbstractService implements ClusterManagerService {

    public AbstractMetaService() {
        this(DEFAULT_RETRY_TIMES, DEFAULT_RETRY_INTERVAL_MILLI);
    }

    public AbstractMetaService(int retryTimes, int retryIntervalMilli) {
        super(retryTimes, retryIntervalMilli);

    }

    protected <T> T pollClusterManager(Function<String, T> fun) {

        List<String> metaServerList = getMetaServerList();

        for (String url : metaServerList) {

            try {
                T result = fun.apply(url);
                if (result != null) {
                    return result;
                }
            } catch (Exception e) {
                logger.error("[pollClusterManager][error poll server]{}", url);
            }
        }
        return null;
    }

    protected abstract List<String> getMetaServerList();

    @Override
    public Replicator getActiveReplicator(final String clusterId) {

        return pollClusterManager(new Function<String, Replicator>() {

            @Override
            public Replicator apply(String metaServerAddress) {


                String activeReplicatorPath = META_SERVER_SERVICE.GET_ACTIVE_REPLICATOR.getRealPath(metaServerAddress);
                Replicator replicator = restTemplate.getForObject(activeReplicatorPath, Replicator.class, clusterId);
                return replicator;
            }

        });
    }
}
