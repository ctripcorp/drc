package com.ctrip.framework.drc.manager.service;

import com.ctrip.framework.drc.manager.config.DataCenterService;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.RegionInfo;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by dengquanliang
 * 2024/10/24 22:24
 */
@Component
public class ReplicatorConsoleNotifier extends AbstractConsoleNotifier implements InitializingBean {

    @Autowired
    private DataCenterService dataCenterService;

    @Autowired
    private ClusterManagerConfig clusterManagerConfig;

    @Override
    public void afterPropertiesSet() throws Exception {
        consoleHosts = getReplicatorConsoleHosts();
        super.startScheduleCheck();
    }

    @Override
    public String type() {
        return "replicatorMasterChanged";
    }

    @Override
    public String getBaseUrl() {
        return "%s/api/drc/v1/switch/clusters/replicators/master/";
    }

    @Override
    public int getNotifySize() {
        return clusterManagerConfig.getConsoleBatchNotifySize();
    }

    private List<String> getReplicatorConsoleHosts() {
        List<String> replicatorConsoleHosts = new ArrayList<>();
        Map<String, RegionInfo> consoleRegionInfos = clusterManagerConfig.getConsoleRegionInfos();
        for (Map.Entry<String, RegionInfo> entry : consoleRegionInfos.entrySet()) {
            String consoleHost = entry.getValue().getMetaServerAddress();
            if (!dataCenterService.getRegion().equalsIgnoreCase(entry.getKey())) {
                replicatorConsoleHosts.add(consoleHost);
            }
        }
        return replicatorConsoleHosts;
    }
}
