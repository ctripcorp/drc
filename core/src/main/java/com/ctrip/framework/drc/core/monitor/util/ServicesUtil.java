package com.ctrip.framework.drc.core.monitor.util;

import com.ctrip.framework.drc.core.driver.config.ConfigSource;
import com.ctrip.framework.drc.core.monitor.reporter.EventMonitor;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.ctrip.framework.drc.core.monitor.reporter.TransactionMonitor;
import com.ctrip.framework.drc.core.mq.DelayMessageConsumer;
import com.ctrip.framework.drc.core.mq.ProducerFactory;
import com.ctrip.framework.drc.core.server.common.filter.service.UserService;
import com.ctrip.framework.drc.core.service.security.HeraldService;
import com.ctrip.framework.drc.core.service.user.IAMService;

/**
 * @Author limingdong
 * @create 2021/10/25
 */
public class ServicesUtil extends com.ctrip.xpipe.utils.ServicesUtil {

    public static Reporter getReportService(){
        return load(Reporter.class);
    }

    public static EventMonitor getEventMonitorService(){
        return load(EventMonitor.class);
    }

    public static TransactionMonitor getTransactionMonitorService(){
        return load(TransactionMonitor.class);
    }

    public static ConfigSource getConfigSourceService(){
        return load(ConfigSource.class);
    }

    public static UserService getUidService(){
        return load(UserService.class);
    }

    public static ProducerFactory getProducerFactory() {
        return load(ProducerFactory.class);
    }

    private static class IAMServiceHolder {
        public static final IAMService INSTANCE = load(IAMService.class);
    }
    public static IAMService getIAMService() {
        return IAMServiceHolder.INSTANCE;
    }

    private static class HeraldServiceHolder {
        public static final HeraldService INSTANCE = load(HeraldService.class);
    }
    public static HeraldService getHeraldService() {
        return HeraldServiceHolder.INSTANCE;
    }


}
