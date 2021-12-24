package com.ctrip.framework.drc.manager.service;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.manager.config.DataCenterService;
import com.ctrip.framework.drc.manager.ha.StateChangeHandler;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.DcInfo;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Map;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.STATE_LOGGER;

/**
 * @Author limingdong
 * @create 2020/7/1
 */
@Order(2)
@Component
public class ConsoleServiceImpl extends AbstractService implements StateChangeHandler {

    @Autowired
    private DataCenterService dataCenterService;

    @Autowired
    private ClusterManagerConfig clusterManagerConfig;

    @Override
    public void replicatorActiveElected(String clusterId, Replicator replicator) {
        if (replicator == null) {
            STATE_LOGGER.info("[{}][replicatorActiveElected][none replicator survive, do nothing]", getClass().getSimpleName());
            return;
        }
        STATE_LOGGER.info("[replicatorActiveElected] for {}:{}", clusterId, replicator);
        Map<String, DcInfo> dcInfoMap = clusterManagerConfig.getConsoleDcInofs();
        for (Map.Entry<String, DcInfo> entry : dcInfoMap.entrySet()) {
            if (!dataCenterService.getDc().equalsIgnoreCase(entry.getKey())) {
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
    public void applierMasterChanged(String clusterReplicatorId, String backupClusterId, Pair<String, Integer> newMaster) {
    }

    @Override
    public void applierActiveElected(String clusterId, Applier applier) {
    }

    @Override
    public void mysqlMasterChanged(String clusterId, Endpoint master) {
        String ipAndPort = master.getHost() + ":" + master.getPort();
        STATE_LOGGER.info("[mysqlMasterChanged] for {}:{}", clusterId, master);
        Map<String, DcInfo> dcInfoMap = clusterManagerConfig.getConsoleDcInofs();
        for (Map.Entry<String, DcInfo> entry : dcInfoMap.entrySet()) {
            String url = entry.getValue().getMetaServerAddress() + "/api/drc/v1/switch/clusters/{clusterId}/dbs/master/";
            if (dataCenterService.getDc().equalsIgnoreCase(entry.getKey())) {
                restTemplate.put(url, ipAndPort, clusterId);
                STATE_LOGGER.info("[mysqlMasterChanged] notify {}, {}", url, clusterId);
                break;
            }
        }
    }

    public String getLocalDbClusters() {
        String localDc = dataCenterService.getDc();
        Map<String, DcInfo> dcInfoMap = clusterManagerConfig.getConsoleDcInofs();

        DcInfo dcInfo = dcInfoMap.get(localDc);
        if(null != dcInfo) {
            String url = String.format(dcInfo.getMetaServerAddress() + "/api/drc/v1/meta/data/dcs/%s", localDc);
            try {
                long s = System.currentTimeMillis();
                String localDbClusters = restTemplate.getForObject(url, String.class);
                long e = System.currentTimeMillis();
                logger.info("[meta] for local dc, took {}ms", e-s);
                return localDbClusters;
            } catch (Exception e) {
                logger.error("[meta] for local dc, ", e);
            }
        }
        return null;
    }
}
