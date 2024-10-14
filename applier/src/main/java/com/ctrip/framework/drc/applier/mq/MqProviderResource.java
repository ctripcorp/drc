package com.ctrip.framework.drc.applier.mq;

import com.ctrip.framework.drc.applier.activity.monitor.MqMetricsActivity;
import com.ctrip.framework.drc.applier.activity.monitor.MqMonitorContext;
import com.ctrip.framework.drc.applier.resource.context.MqTransactionContextResource;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.mq.Producer;
import com.ctrip.framework.drc.core.mq.MessengerProperties;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.fetcher.system.AbstractResource;
import com.ctrip.framework.drc.fetcher.system.InstanceActivity;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by jixinwang on 2022/10/17
 */
public class MqProviderResource extends AbstractResource implements MqProvider {

    private static final Logger loggerMsg = LoggerFactory.getLogger("MESSENGER");

    @InstanceConfig(path = "properties")
    public String properties;

    @InstanceConfig(path = "registryKey")
    public String registryKey;

    private MessengerProperties messengerProperties;

    private ScheduledExecutorService scheduledExecutorService;

    @InstanceActivity
    public MqMetricsActivity reporter;

    @Override
    protected void doInitialize() throws Exception {
        messengerProperties = MessengerProperties.from(properties);

        scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor(registryKey);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            if(!Thread.currentThread().isInterrupted()) {
                int active = MqTransactionContextResource.getConcurrency(registryKey);
                if (reporter != null) {
                    MqMonitorContext mqMonitorContext = new MqMonitorContext(active, registryKey, "fx.drc.messenger.active");
                    reporter.report(mqMonitorContext);
                }
            }
        }, 100, 200, TimeUnit.MILLISECONDS);
    }

    @Override
    public List<Producer> getProducers(String tableName) {
        List<Producer> producers = messengerProperties.getProducers(tableName);
        if (producers.isEmpty()) {
            loggerMsg.error("[MQ][{}] get empty producers for table: {}", registryKey, tableName);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.mq.producer.empty", tableName);
        }
        return producers;
    }

    @Override
    protected void doDispose() throws Exception {
        Map<String, List<Producer>> regex2Producers = messengerProperties.getRegex2Producers();
        for (Map.Entry<String, List<Producer>> entry : regex2Producers.entrySet()) {
            List<Producer> producers = entry.getValue();
            for (Producer producer : producers) {
                producer.destroy();
            }
        }

        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdown();
            scheduledExecutorService = null;
        }
    }
}
