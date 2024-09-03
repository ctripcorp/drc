package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.enums.BroadcastEnum;
import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.task.ListenReplicatorTask;
import com.ctrip.framework.drc.console.service.SwitchService;
import com.ctrip.framework.drc.console.service.v2.DbMetaCorrectService;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import com.ctrip.framework.drc.console.service.broadcast.HttpNotificationBroadCast;
import org.springframework.web.bind.annotation.RequestMethod;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-03-05
 */
@Service
public class SwitchServiceImpl implements SwitchService {

    private final Logger logger = LoggerFactory.getLogger("delayMonitorLogger");

    @Autowired 
    private DefaultCurrentMetaManager currentMetaManager;

    @Autowired 
    private ListenReplicatorTask listenReplicatorTask;

    @Autowired 
    private DbMetaCorrectService dbMetaCorrectService;
    
    @Autowired 
    private HttpNotificationBroadCast httpBoardCast;

    @Override
    public void switchUpdateDb(String registryKey, String endpoint,boolean firstHand) {
        String[] split = endpoint.split(":");
        String ip = split[0];
        int port = Integer.parseInt(split[1]);
        currentMetaManager.updateMasterMySQL(registryKey, new DefaultEndPoint(ip, port));
        if (firstHand) {
            dbMetaCorrectService.mhaMasterDbChange(RegistryKey.from(registryKey).getMhaName(), ip, port);
            String urlPath = String.format(BroadcastEnum.MYSQL_MASTER_CHANGE.getPath(), registryKey);
            httpBoardCast.broadcast(urlPath, RequestMethod.PUT,endpoint);
        }
    }

    @Async
    @Override
    public void switchListenReplicator(String clusterId, String endpoint,boolean firstHand) {
        logger.info("[HTTP] start switch listen replicator for clusterId: {} with endpoint: {}", clusterId, endpoint);
        String[] split = endpoint.split(":");
        listenReplicatorTask.switchListenReplicator(clusterId, split[0], Integer.parseInt(split[1]));
        logger.info("[HTTP] end switch listen replicator for clusterId: {} with endpoint: {}", clusterId, endpoint);
        if (firstHand) {
            String urlPath = String.format(BroadcastEnum.REPLICATOR_CHANGE.getPath(), clusterId);
            httpBoardCast.broadcast(urlPath, RequestMethod.PUT,endpoint);
        }
    }
}
