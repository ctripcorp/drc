package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.core.server.config.console.dto.ClusterConfigDto;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-03-05
 */
public interface SwitchService {
    void switchUpdateDb(ClusterConfigDto clusterConfigDto) throws Exception;

    void switchListenReplicator(ClusterConfigDto clusterConfigDto);

    void switchListenMessenger(ClusterConfigDto clusterConfigDto);
}
