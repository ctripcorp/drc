package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.task.ListenReplicatorTask;
import com.ctrip.framework.drc.console.service.SwitchService;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.config.console.dto.DbEndpointDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-03-05
 */
@Service
public class SwitchServiceImpl implements SwitchService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private DefaultCurrentMetaManager currentMetaManager;

    @Autowired
    private ListenReplicatorTask listenReplicatorTask;

    @Autowired
    private DrcMaintenanceServiceImpl drcMaintenanceService;

    @Override
    public ApiResult switchUpdateDb(String registryKey, DbEndpointDto dbEndpointDto) {
        currentMetaManager.updateMasterMySQL(registryKey, new DefaultEndPoint(dbEndpointDto.getIp(), dbEndpointDto.getPort()));
        return drcMaintenanceService.changeMasterDb(RegistryKey.from(registryKey).getMhaName(), dbEndpointDto.getIp(), dbEndpointDto.getPort());
    }

    @Async
    @Override
    public void switchListenReplicator(String clusterId, String endpoint) {
        listenReplicatorTask.checkMaster(clusterId, endpoint);
    }
}
