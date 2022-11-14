package com.ctrip.framework.drc.applier.mq;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.mq.Producer;
import com.ctrip.framework.drc.core.mq.MessengerProperties;
import com.ctrip.framework.drc.fetcher.system.AbstractResource;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

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

    @Override
    protected void doInitialize() throws Exception {
        messengerProperties = MessengerProperties.from(properties);
    }

    @Override
    public List<Producer> getProducers(String tableName) {
        List<Producer> producers = messengerProperties.getProducers(tableName);
        if (producers.isEmpty()) {
            loggerMsg.error("[MQ][{}] get empty producers for table: {}", registryKey, tableName);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.mq.produce.get.empty", tableName);
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
    }
}
