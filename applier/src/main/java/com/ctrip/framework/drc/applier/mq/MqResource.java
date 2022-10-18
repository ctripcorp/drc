package com.ctrip.framework.drc.applier.mq;

import com.ctrip.framework.drc.core.mq.IProducer;
import com.ctrip.framework.drc.core.mq.MessengerProperties;
import com.ctrip.framework.drc.fetcher.system.AbstractResource;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;

import java.util.List;

/**
 * Created by jixinwang on 2022/10/17
 */
public class MqResource extends AbstractResource implements Mq {

    @InstanceConfig(path = "properties")
    public String properties;

    MessengerProperties messengerProperties;

    @Override
    protected void doInitialize() throws Exception {
        messengerProperties = MessengerProperties.from(properties);
    }

    @Override
    public List<IProducer> getProducers(String tableName) {
        return messengerProperties.getProducers(tableName);
    }
}
