package com.ctrip.framework.drc.manager.healthcheck.notifier;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Instance;

/**
 * Created by mingdongli
 * 2019/11/22 上午12:35.
 */
public interface Notifier {

    /**
     * invoke post when mysql switch
     * @param dbCluster
     */
    void notify(String clusterId, DbCluster dbCluster);

    /**
     * invoke put when init or instance restart
     * @param dbCluster
     */
    void notifyAdd(String clusterId, DbCluster dbCluster);

    /**
     * invoke put when init or instance restart
     * @param clusterId
     */
    void notifyRemove(String clusterId, Instance instance, boolean delete);

    /**
     * invoke put when init or instance restart
     * @param dbCluster
     */
    void notifyRegister(String clusterId, DbCluster dbCluster);
}
