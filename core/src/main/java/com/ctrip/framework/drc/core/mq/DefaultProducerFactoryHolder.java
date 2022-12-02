package com.ctrip.framework.drc.core.mq;

import com.ctrip.framework.drc.core.monitor.util.ServicesUtil;

/**
 * Created by jixinwang on 2022/10/19
 */
public class DefaultProducerFactoryHolder {

    private static class ProducerFactoryHolder {
        public static final ProducerFactory INSTANCE = ServicesUtil.getProducerFactory();
    }

    public static ProducerFactory getInstance() {
        return DefaultProducerFactoryHolder.ProducerFactoryHolder.INSTANCE;
    }
}
