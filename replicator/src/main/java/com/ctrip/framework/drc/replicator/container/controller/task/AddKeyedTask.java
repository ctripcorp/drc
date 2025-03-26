package com.ctrip.framework.drc.replicator.container.controller.task;

import com.ctrip.framework.drc.core.driver.config.InstanceStatus;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.core.server.container.ServerContainer;
import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * Created by jixinwang on 2023/6/25
 */
public class AddKeyedTask extends AbstractKeyedTask {

    public AddKeyedTask(String registryKey, ReplicatorConfig replicatorConfig, ServerContainer<ReplicatorConfig, ApiResult> serverContainer) {
        super(registryKey, replicatorConfig, serverContainer);
    }

    @Override
    protected void doExecute() {
        try {
            if (checkConfig()) {
                removeOldInstance();
                addNewInstance();
                logger.info("[Start] replicator instance({}) success", registryKey);
            }
            future().setSuccess();
        } catch (Throwable t) {
            logger.error("[Start] replicator instance({}) error", registryKey, t);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.instance.error", "start");
            future().setFailure(t);
        }
    }

    private boolean checkConfig() {
        InstanceStatus instanceStatus = InstanceStatus.getInstanceStatus(replicatorConfig.getStatus());
        logger.info("[Start] replicator instance({}) with role: {}", registryKey, instanceStatus);
        Endpoint upstreamMaster = serverContainer.getUpstreamMaster(registryKey);
        Endpoint currentUpstreamMaster = replicatorConfig.getEndpoint();
        logger.info("[Start] replicator instance({}), upstreamMaster is {}, current is {}", registryKey, upstreamMaster, currentUpstreamMaster);
        if (upstreamMaster != null && upstreamMaster.equals(currentUpstreamMaster) && equalsWithUserAndPassword(upstreamMaster, currentUpstreamMaster)) {
            logger.info("[Start] replicator instance({}), ignore duplicate instance, with role: {}", registryKey, instanceStatus);
            return false;
        }
        return true;
    }

    private boolean equalsWithUserAndPassword(Endpoint endpoint1, Endpoint endpoint2) {
        if (endpoint1 == null && endpoint2 == null) {
            return true;
        }
        if (endpoint1 == null || endpoint2 == null) {
            return false;
        }
        if (!endpoint1.getUser().equals(endpoint2.getUser())) {
            return false;
        }
        return endpoint1.getPassword().equals(endpoint2.getPassword());
    }

    private void removeOldInstance() {
        logger.info("[Start] replicator instance({}), remove old instance start", registryKey);
        long start = System.currentTimeMillis();
        serverContainer.removeServer(registryKey, false);
        logger.info("[Start] replicator instance({}), remove old instance end, cost: {}ms", registryKey, (System.currentTimeMillis() - start));
    }

    private void addNewInstance() {
        logger.info("[Start] replicator instance({}), add new instance start", registryKey);
        long start = System.currentTimeMillis();
        serverContainer.addServer(replicatorConfig);
        logger.info("[Start] replicator instance({}), add new instance end, cost: {}ms", registryKey, (System.currentTimeMillis() - start));
    }
}
