package com.ctrip.framework.drc.applier.mq;

import com.ctrip.framework.drc.core.mq.Producer;
import com.ctrip.framework.drc.core.mq.MessengerProperties;
import com.ctrip.framework.drc.fetcher.system.AbstractResource;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;

import java.util.List;

/**
 * Created by jixinwang on 2022/10/17
 */
public class MqProviderResource extends AbstractResource implements MqProvider {

    @InstanceConfig(path = "properties")
    public String properties;

    MessengerProperties messengerProperties;

    @Override
    protected void doInitialize() throws Exception {
        messengerProperties = MessengerProperties.from(properties);
    }

    @Override
    public List<Producer> getProducers(String tableName) {
        //TODO: 延迟监控，考虑其他监控参数
        return messengerProperties.getProducers(tableName);
    }
}
