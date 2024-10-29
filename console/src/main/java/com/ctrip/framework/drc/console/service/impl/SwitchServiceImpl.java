package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.enums.BroadcastEnum;
import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.task.ListenReplicatorTask;
import com.ctrip.framework.drc.console.service.SwitchService;
import com.ctrip.framework.drc.console.service.broadcast.HttpNotificationBroadCast;
import com.ctrip.framework.drc.console.service.v2.DbMetaCorrectService;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.config.console.dto.ClusterConfigDto;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestMethod;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    private ListeningExecutorService executorService = MoreExecutors.listeningDecorator(ThreadUtils.newCachedThreadPool("SwitchServiceImpl"));
    private static final int RETRY_TIME = 1;

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

    @Override
    public void switchUpdateDb(ClusterConfigDto clusterConfigDto) throws Exception {
        List<MhaInstanceGroupDto> mhaInstanceGroupDtos = new ArrayList<>();
        for (Map.Entry<String, String> entry : clusterConfigDto.getClusterMap().entrySet()) {
            String cluster = entry.getKey();
            String endpoint = entry.getValue();
            String[] split = endpoint.split(":");
            String ip = split[0];
            int port = Integer.parseInt(split[1]);

            mhaInstanceGroupDtos.add(buildMhaInstanceGroupDto(cluster, ip, port));

            logger.info("switchUpdateDb firstHand:{}, clusterId: {}, endpoint: {}", clusterConfigDto.isFirstHand(), entry.getKey(), entry.getValue());
            executorService.submit(() -> currentMetaManager.updateMasterMySQL(cluster, new DefaultEndPoint(ip, port)));
        }
        if (clusterConfigDto.isFirstHand()) {
            dbMetaCorrectService.batchMhaMasterDbChange(mhaInstanceGroupDtos);
            ClusterConfigDto copyDto = new ClusterConfigDto(clusterConfigDto.getClusterMap(), false);
            executorService.submit(() -> httpBoardCast.broadcastWithRetry(BroadcastEnum.MYSQL_MASTER_CHANGE_V2.getPath(), RequestMethod.PUT, JsonUtils.toJson(copyDto), RETRY_TIME));
        }
    }

    @Override
    public void switchListenReplicator(ClusterConfigDto clusterConfigDto) {
        for (Map.Entry<String, String> entry : clusterConfigDto.getClusterMap().entrySet()) {
            logger.info("switchListenReplicator firstHand:{}, clusterId: {}, endpoint: {}", clusterConfigDto.isFirstHand(), entry.getKey(), entry.getValue());
            String[] split = entry.getValue().split(":");
            executorService.submit(() -> listenReplicatorTask.switchListenReplicator(entry.getKey(), split[0], Integer.parseInt(split[1])));
        }
        if (clusterConfigDto.isFirstHand()) {
            ClusterConfigDto copyDto = new ClusterConfigDto(clusterConfigDto.getClusterMap(), false);
            executorService.submit(() -> httpBoardCast.broadcastWithRetry(BroadcastEnum.REPLICATOR_CHANGE_V2.getPath(), RequestMethod.PUT, JsonUtils.toJson(copyDto), RETRY_TIME));
        }
    }

    private MhaInstanceGroupDto buildMhaInstanceGroupDto(String cluster, String ip, int port) {
        MhaInstanceGroupDto mhaInstanceGroupDto = new MhaInstanceGroupDto();
        mhaInstanceGroupDto.setMhaName(RegistryKey.from(cluster).getMhaName());
        MhaInstanceGroupDto.MySQLInstance mysqlMaster = new MhaInstanceGroupDto.MySQLInstance();
        mysqlMaster.setIp(ip);
        mysqlMaster.setPort(port);
        mhaInstanceGroupDto.setMaster(mysqlMaster);

        return mhaInstanceGroupDto;
    }

}
