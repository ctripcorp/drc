package com.ctrip.framework.drc.core.monitor.kpi;

import com.ctrip.framework.drc.core.monitor.entity.UnidirectionalEntity;
import com.ctrip.framework.drc.core.monitor.entity.TrafficEntity;
import com.ctrip.framework.drc.core.monitor.util.IsolateHashCache;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.ExecutorService;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DELAY_LOGGER;

/**
 * @Author limingdong
 * @create 2020/3/17
 */
public class DelayMonitorReport extends AbstractMonitorResource {

    private static final String DRC_REPLICATOR_DELAY_MEASUREMENT = "fx.drc.replicator.delay";

    private static final int MAX_SIZE = 1000;   //for every gtid

    private ExecutorService sendDelayExecutorService;

    private UnidirectionalEntity unidirectionalEntity;

    private IsolateHashCache<String, Long> gtidMap;

    private IsolateHashCache<String, String> monitorGtidMap;

    public DelayMonitorReport(long domain, TrafficEntity trafficEntity) {
        clusterName = trafficEntity.getClusterName();
        unidirectionalEntity = new UnidirectionalEntity.Builder()
                .clusterAppId(domain)
                .buName(trafficEntity.getBuName())
                .srcDcName(trafficEntity.getDcName())
                .destDcName(trafficEntity.getDcName())
                .clusterName(clusterName)
                .build();
    }

    @Override
    protected void doInitialize() throws Exception {
        gtidMap = new IsolateHashCache<>(MAX_SIZE, 16, 4);

        monitorGtidMap = new IsolateHashCache<>(MAX_SIZE, 16, 4);

        sendDelayExecutorService = ThreadUtils.newSingleThreadExecutor(getClass().getSimpleName() + "-send-" + clusterName);
    }

    @Override
    protected void doDispose() throws Exception {
        sendDelayExecutorService.shutdown();
    }

    public void receiveGtid(String gtid) {
        gtidMap.put(gtid, System.currentTimeMillis());
    }

    public void receiveMonitorGtid(String gtid) {
        if (StringUtils.isNotBlank(gtid)) {
            monitorGtidMap.put(gtid, "");
        }
    }

    public void sendGtid(String gtid) {
        sendDelayExecutorService.submit(() -> {
            try {
                long now = System.currentTimeMillis();
                Long receiveTime =  gtidMap.getIfPresent(gtid);
                if (receiveTime == null) {
                    if (DELAY_LOGGER.isDebugEnabled()) {
                        DELAY_LOGGER.debug("[Delay] reset for {} to now and delay 0ms", gtid);
                    }
                    return;
                }
                gtidMap.invalidate(gtid);

                long delay = now - receiveTime;
                if (delay > 5) {
                    if (DELAY_LOGGER.isDebugEnabled()) {
                        DELAY_LOGGER.debug("[Slow] delay for {} is {}ms ", gtid, delay);
                    }
                }
                if (monitorGtidMap.getIfPresent(gtid) != null) {
                    hickwallReporter.reportDelay(unidirectionalEntity, delay, DRC_REPLICATOR_DELAY_MEASUREMENT);
                    if (DELAY_LOGGER.isDebugEnabled()) {
                        DELAY_LOGGER.debug("[Hickwall] report for {}", gtid);
                    }
                    monitorGtidMap.invalidate(gtid);
                }
            } catch (Exception e) {
                DELAY_LOGGER.error("reportDelay error", e);
            }
        });

    }
}
