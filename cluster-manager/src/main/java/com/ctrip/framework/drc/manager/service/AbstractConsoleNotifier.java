package com.ctrip.framework.drc.manager.service;

import com.ctrip.framework.drc.core.http.AsyncHttpClientFactory;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.server.config.console.dto.ClusterConfigDto;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.STATE_LOGGER;
import static com.ctrip.framework.drc.manager.service.AbstractService.DEFAULT_CONNECT_TIMEOUT;
import static com.ctrip.framework.drc.manager.service.AbstractService.DEFAULT_SO_TIMEOUT;

/**
 * Created by dengquanliang
 * 2024/10/23 11:42
 */
public abstract class AbstractConsoleNotifier {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private final Object lock = new Object();
    private Map<String, String> clusterMap = new HashMap<>();
    private static final int RETRY_TIME = 1;

    public List<String> consoleHosts;

    private ScheduledExecutorService executorService = ThreadUtils.newSingleThreadScheduledExecutor("CONSOLE-Notify-Schedule");

    private AsyncHttpClient asyncHttpClient = AsyncHttpClientFactory.create(DEFAULT_CONNECT_TIMEOUT, DEFAULT_SO_TIMEOUT, DEFAULT_SO_TIMEOUT, 0, 100, 1000);

    public abstract String type();

    public abstract String getBaseUrl();

    public abstract int getNotifySize();

    public abstract boolean getCmBatchNotifyConsoleSwitch();

    public List<String> getConsoleHosts() {
        return consoleHosts;
    };

    public void startScheduleCheck() {
        executorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                if (!getCmBatchNotifyConsoleSwitch()) {
                    STATE_LOGGER.info("[{}] schedule notify, switch is false", type());
                    return;
                }
                Map<String, String> clusters = new HashMap<>();
                synchronized (lock) {
                    if (clusterMap.size() > 0) {
                        STATE_LOGGER.info("[{}] schedule notify, size: {}, {}", type(), clusterMap.size(), clusterMap);
                        clusters = new HashMap<>(clusterMap);
                        clusterMap.clear();
                    }
                }
                if (clusters.size() > 0) {
                    Map<String, String> clusterMap = new HashMap<>(clusters);
                    DefaultTransactionMonitorHolder.getInstance().logTransactionSwallowException(String.format("DRC.cm.%s", type()), String.format("schedule.notify.%s", clusters.size()), () -> {
                        notifyMasterChanged(clusterMap);
                    });

                }

            }
        }, 0, 1000, TimeUnit.MILLISECONDS);
    }

    public void notifyMasterChanged(String clusterId, String ipAndPort) {
        Map<String, String> clusters;
        synchronized (lock) {
            STATE_LOGGER.info("[{}] add cluster {}, {} to clusterMap", type(), clusterId, ipAndPort);
            clusterMap.put(clusterId, ipAndPort);
            if (clusterMap.size() < getNotifySize()) {
                STATE_LOGGER.info("[{}] not ready to notify, current size: {}, expected size: {}", type(), clusterMap.size(), getNotifySize());
                return;
            }
            STATE_LOGGER.info("[{}] batch notify, size: {}, {}", type(), clusterMap.size(), clusterMap);
            clusters = new HashMap<>(clusterMap);
            clusterMap.clear();

        }
        Map<String, String> clusterMap = new HashMap<>(clusters);
        DefaultTransactionMonitorHolder.getInstance().logTransactionSwallowException(String.format("DRC.cm.%s", type()), String.format("batch.notify.%s", clusters.size()), () -> {
            notifyMasterChanged(clusterMap);
        });
    }

    private void notifyMasterChanged(Map<String, String> clusters) {
        STATE_LOGGER.info("[{}] notify, size: {}, {}", type(), clusters.size(), clusters);
        for (String consoleHost : getConsoleHosts()) {
            String url = String.format(getBaseUrl(), consoleHost);
            ClusterConfigDto clusterConfigDto = new ClusterConfigDto(clusters, true);
            STATE_LOGGER.info("[{}}] clusterConfigDto: {}", type(), clusterConfigDto);
            asyncHttpNotify(clusterConfigDto, url, RETRY_TIME);
        }
    }

    private void asyncHttpNotify(ClusterConfigDto clusterConfigDto, String url, int retryTime) {
        ListenableFuture<Response> httpFuture = asyncHttpClient.preparePut(url)
                .setHeader("Accept", "application/json")
                .setHeader("Content-Type", "application/json; charset=utf-8")
                .setBody(JsonUtils.toJson(clusterConfigDto)).execute();
        int nextRetryTime = retryTime - 1;
        httpFuture.addListener(() -> {
            try {
                Response response = httpFuture.get();
                if (response.getStatusCode() != 200) {
                    logger.error("[{}] fail for {}, retryTime: {}, {}", type(), url, retryTime, response.getResponseBody());
                    if (retryTime > 0) {
                        asyncHttpNotify(clusterConfigDto, url, nextRetryTime);
                    }

                }
                STATE_LOGGER.info("[{}] notify success {}, size: {}, {}", type(), url, clusterConfigDto.getClusterMap().size(), clusterConfigDto.getClusterMap());
            } catch (Throwable t) {
                logger.error("[{}] error for {}, retryTime: {}", type(), url, retryTime, t);
                if (retryTime > 0) {
                    asyncHttpNotify(clusterConfigDto, url, nextRetryTime);
                }
            }
        }, MoreExecutors.directExecutor());
    }

    @VisibleForTesting
    protected Map<String, String> getClusterMap() {
        return clusterMap;
    }

}
