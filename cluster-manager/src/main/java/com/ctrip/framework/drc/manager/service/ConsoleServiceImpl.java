package com.ctrip.framework.drc.manager.service;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.monitor.util.ServicesUtil;
import com.ctrip.framework.drc.core.service.security.HeraldService;
import com.ctrip.framework.drc.manager.config.DataCenterService;
import com.ctrip.framework.drc.manager.ha.StateChangeHandler;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.RegionInfo;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Map;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.META_LOGGER;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.STATE_LOGGER;

/**
 * @Author limingdong
 * @create 2020/7/1
 */
@Order(3)
@Component
public class ConsoleServiceImpl extends AbstractService implements StateChangeHandler {

    @Autowired
    private DataCenterService dataCenterService;

    @Autowired
    private ClusterManagerConfig clusterManagerConfig;
    
    private HeraldService heraldService = ServicesUtil.getHeraldService();

    @Override
    public void replicatorActiveElected(String clusterId, Replicator replicator) {
        if (replicator == null) {
            STATE_LOGGER.info("[{}][replicatorActiveElected][none replicator survive, do nothing]", getClass().getSimpleName());
            return;
        }
        STATE_LOGGER.info("[replicatorActiveElected] for {}:{}", clusterId, replicator);
        Map<String, RegionInfo> consoleRegionInfos = clusterManagerConfig.getConsoleRegionInfos();
        for (Map.Entry<String, RegionInfo> entry : consoleRegionInfos.entrySet()) {
            if (!dataCenterService.getRegion().equalsIgnoreCase(entry.getKey())) {
                String url = entry.getValue().getMetaServerAddress() + "/api/drc/v1/switch/clusters/{clusterId}/replicators/master/";
                try {
                    String ipAndPort = replicator.getIp() + ":" + replicator.getApplierPort();
                    restTemplate.put(url, ipAndPort, clusterId);
                    STATE_LOGGER.info("[replicatorActiveElected] notify {}, {}", url, clusterId);
                } catch (Throwable t) {
                    logger.error("[replicatorActiveElected] error for {}", url, t);
                }
            }
        }
    }

    @Override
    public void messengerActiveElected(String clusterId, Messenger messenger) {

    }

    @Override
    public void applierMasterChanged(String clusterReplicatorId, String backupClusterId, Pair<String, Integer> newMaster) {
    }

    @Override
    public void applierActiveElected(String clusterId, Applier applier) {
    }

    @Override
    public void mysqlMasterChanged(String clusterId, Endpoint master) {
        String ipAndPort = master.getHost() + ":" + master.getPort();
        STATE_LOGGER.info("[mysqlMasterChanged] for {}:{}", clusterId, master);
        Map<String, RegionInfo> consoleRegionInfos = clusterManagerConfig.getConsoleRegionInfos();
        for (Map.Entry<String, RegionInfo> entry : consoleRegionInfos.entrySet()) {
            String url = entry.getValue().getMetaServerAddress() + "/api/drc/v1/switch/clusters/{clusterId}/dbs/master/";
            if (dataCenterService.getRegion().equalsIgnoreCase(entry.getKey())) {
                restTemplate.put(url, ipAndPort, clusterId);
                STATE_LOGGER.info("[mysqlMasterChanged] notify {}, {}", url, clusterId);
                break;
            }
        }
    }

    public String getDbClusters(String dcId) {
        Map<String, RegionInfo> consoleRegionInfos = clusterManagerConfig.getConsoleRegionInfos();
        String region = dataCenterService.getRegion(dcId);
        RegionInfo regionInfo = consoleRegionInfos.get(region);
        if (null != regionInfo) {
            String url  = String.format(regionInfo.getMetaServerAddress() + "/api/drc/v2/meta/data/dcs/%s?refresh=true", dcId);
            if (clusterManagerConfig.requestWithHeraldToken()) {
                url += "&heraldToken=" + heraldService.getLocalHeraldToken();
            }
            META_LOGGER.info("[meta] for dc: {} using realtime url: {}", dcId, url);
            try {
                String finalUrl = url;
                return DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.meta.get", region, () -> {
                    long s = System.currentTimeMillis();
                    String dbClusters = restTemplate.getForObject(finalUrl, String.class);
                    long e = System.currentTimeMillis();
                    META_LOGGER.info("[meta] for dc: {}, took {}ms", dcId, e - s);
                    META_LOGGER.debug("[meta] for dc: {}, info: {}", dcId, dbClusters);
                    return dbClusters;
                });
            } catch (Exception e) {
                META_LOGGER.error("[meta] for dc: {}, ", dcId, e);
            }
        }
        return null;
    }
}
