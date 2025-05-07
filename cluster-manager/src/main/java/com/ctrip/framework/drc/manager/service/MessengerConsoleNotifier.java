package com.ctrip.framework.drc.manager.service;

import com.ctrip.framework.drc.manager.config.DataCenterService;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.RegionInfo;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Created by dengquanliang
 * 2025/2/24 16:16
 */
@Component
public class MessengerConsoleNotifier extends AbstractConsoleNotifier implements InitializingBean {

    @Autowired
    private DataCenterService dataCenterService;

    @Autowired
    private ClusterManagerConfig clusterManagerConfig;

    @Override
    public void afterPropertiesSet() throws Exception {
        consoleHosts = getMessengerConsoleHosts();
        super.startScheduleCheck();
    }

    @Override
    public String type() {
        return "messengerMasterChanged";
    }

    @Override
    public String getBaseUrl() {
        return "%s/api/drc/v1/switch/clusters/messengers/master/";
    }

    @Override
    public int getNotifySize() {
        return clusterManagerConfig.getConsoleBatchNotifySize();
    }

    private List<String> getMessengerConsoleHosts() {
        String localRegion = dataCenterService.getRegion();

        Map<String, RegionInfo> consoleRegionInfos = clusterManagerConfig.getConsoleRegionInfos();
        String consoleHost = consoleRegionInfos.get(localRegion).getMetaServerAddress();
        return Lists.newArrayList(consoleHost);
    }
}
