package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.task.ListenReplicatorTask;
import com.ctrip.framework.drc.console.service.SwitchService;
import com.ctrip.framework.drc.console.service.v2.MetaDbCorrectService;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.config.console.dto.DbEndpointDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-03-05
 */
@Service
public class SwitchServiceImpl implements SwitchService {

    private final Logger logger = LoggerFactory.getLogger("delayMonitorLogger");

    @Autowired private DefaultCurrentMetaManager currentMetaManager;

    @Autowired private ListenReplicatorTask listenReplicatorTask;

    @Autowired private MetaDbCorrectService metaDbCorrectService;

    @Override
    public ApiResult switchUpdateDb(String registryKey, DbEndpointDto dbEndpointDto) {
        currentMetaManager.updateMasterMySQL(registryKey, new DefaultEndPoint(dbEndpointDto.getIp(), dbEndpointDto.getPort()));
        return metaDbCorrectService.mhaMasterDbChange(RegistryKey.from(registryKey).getMhaName(), dbEndpointDto.getIp(), dbEndpointDto.getPort());
    }

    @Async
    @Override
    public void switchListenReplicator(String clusterId, String endpoint) {
        logger.info("[HTTP] start switch listen replicator for clusterId: {} with endpoint: {}", clusterId, endpoint);
        String[] split = endpoint.split(":");
        if (split.length == 2) {
            listenReplicatorTask.switchListenReplicator(clusterId, split[0], Integer.parseInt(split[1]));
        }
        logger.info("[HTTP] end switch listen replicator for clusterId: {} with endpoint: {}", clusterId, endpoint);
    }
}
