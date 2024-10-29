package com.ctrip.framework.drc.manager.service;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.http.AsyncHttpClientFactory;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.monitor.util.ServicesUtil;
import com.ctrip.framework.drc.core.service.security.HeraldService;
import com.ctrip.framework.drc.manager.config.DataCenterService;
import com.ctrip.framework.drc.manager.ha.StateChangeHandler;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.RegionInfo;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.util.concurrent.MoreExecutors;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.META_LOGGER;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.STATE_LOGGER;

/**
 * @Author limingdong
 * @create 2020/7/1
 */
@Order(3)
@Component
public class ConsoleServiceImpl extends AbstractService implements StateChangeHandler, DisposableBean {

    @Autowired
    private DataCenterService dataCenterService;

    @Autowired
    private ClusterManagerConfig clusterManagerConfig;

    @Autowired
    private MysqlConsoleNotifier mysqlConsoleNotifier;

    @Autowired
    private ReplicatorConsoleNotifier replicatorConsoleNotifier;

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private static final String REPLICATOR_ACTIVE_ELECTED = "%s/api/drc/v1/switch/clusters/%s/replicators/master/";
    private static final String MYSQL_MASTER_CHANGE = "%s/api/drc/v1/switch/clusters/%s/dbs/master/";
    private HeraldService heraldService = ServicesUtil.getHeraldService();

    private AsyncHttpClient asyncHttpConsoleNotifier = AsyncHttpClientFactory.create(DEFAULT_CONNECT_TIMEOUT, DEFAULT_SO_TIMEOUT, DEFAULT_SO_TIMEOUT, 0, 100, 1000);

    @Override
    public void replicatorActiveElected(String clusterId, Replicator replicator) {
        if (replicator == null) {
            STATE_LOGGER.info("[{}][replicatorActiveElected][none replicator survive, do nothing]", getClass().getSimpleName());
            return;
        }
        if (clusterManagerConfig.getCmBatchNotifyConsoleSwitch()) {
            STATE_LOGGER.info("[replicatorMasterChanged] notify to console: {}, {}", clusterId, replicator);
            String ipAndPort = replicator.getIp() + ":" + replicator.getApplierPort();
            replicatorConsoleNotifier.notifyMasterChanged(clusterId, ipAndPort);
        } else {
            notifyReplicatorMasterChanged(clusterId, replicator);
        }
    }


    @Override
    public void mysqlMasterChanged(String clusterId, Endpoint master) {
        if (clusterManagerConfig.getCmBatchNotifyConsoleSwitch()) {
            STATE_LOGGER.info("[mysqlMasterChanged] notify to console: {}, {}", clusterId, master);
            String ipAndPort = master.getHost() + ":" + master.getPort();
            mysqlConsoleNotifier.notifyMasterChanged(clusterId, ipAndPort);
        } else {
            notifyMysqlMasterChanged(clusterId, master);
        }
    }


    private void notifyMysqlMasterChanged(String clusterId, Endpoint master) {
        String ipAndPort = master.getHost() + ":" + master.getPort();
        STATE_LOGGER.info("[mysqlMasterChanged] for {}:{}", clusterId, master);
        Map<String, RegionInfo> consoleRegionInfos = clusterManagerConfig.getConsoleRegionInfos();
        for (Map.Entry<String, RegionInfo> entry : consoleRegionInfos.entrySet()) {
            String consoleHost = entry.getValue().getMetaServerAddress();
            String url = String.format(MYSQL_MASTER_CHANGE, consoleHost, clusterId);
            if (dataCenterService.getRegion().equalsIgnoreCase(entry.getKey())) {
                ListenableFuture<Response> httpFuture = asyncHttpConsoleNotifier.preparePut(url).setBody(ipAndPort).execute();
                httpFuture.addListener(
                        () -> {
                            try {
                                Response response = httpFuture.get();
                                if (response.getStatusCode() != 200) {
                                    STATE_LOGGER.error("[mysqlMasterChanged] error for {}, {}", url, response.getResponseBody());
                                }
                                STATE_LOGGER.info("[mysqlMasterChanged] notify {}, {}", url, clusterId);
                            } catch (Throwable t) {
                                STATE_LOGGER.error("[mysqlMasterChanged] error for {}", url, t);
                            }
                        }, MoreExecutors.directExecutor());
                break;
            }
        }
    }

    private void notifyReplicatorMasterChanged(String clusterId, Replicator replicator) {
        STATE_LOGGER.info("[replicatorActiveElected] for {}:{}", clusterId, replicator);
        Map<String, RegionInfo> consoleRegionInfos = clusterManagerConfig.getConsoleRegionInfos();
        for (Map.Entry<String, RegionInfo> entry : consoleRegionInfos.entrySet()) {
            if (!dataCenterService.getRegion().equalsIgnoreCase(entry.getKey())) {
                String host = entry.getValue().getMetaServerAddress();
                String url = String.format(REPLICATOR_ACTIVE_ELECTED, host, clusterId);
                try {
                    String ipAndPort = replicator.getIp() + ":" + replicator.getApplierPort();
                    ListenableFuture<Response> httpFuture = asyncHttpConsoleNotifier.preparePut(url).setBody(ipAndPort).execute();
                    httpFuture.addListener(() -> {
                        try {
                            Response response = httpFuture.get();
                            if (response.getStatusCode() != 200) {
                                STATE_LOGGER.error("[replicatorActiveElected] error for {}, {}", url, response.getResponseBody());
                            }
                            STATE_LOGGER.info("[replicatorActiveElected] notify {}, {}", url, clusterId);
                        } catch (Throwable t) {
                            STATE_LOGGER.error("[replicatorActiveElected] error for {}", url, t);
                        }
                    }, MoreExecutors.directExecutor());
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


    public String getDbClusters(String dcId) {
        Map<String, RegionInfo> consoleRegionInfos = clusterManagerConfig.getConsoleRegionInfos();
        String region = dataCenterService.getRegion(dcId);
        RegionInfo regionInfo = consoleRegionInfos.get(region);
        if (null != regionInfo) {
            String url = String.format(regionInfo.getMetaServerAddress() + "/api/drc/v2/meta/data/dcs/%s?refresh=true", dcId);
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

    @Override
    public void destroy() throws Exception {
        asyncHttpConsoleNotifier.close();
    }

}
