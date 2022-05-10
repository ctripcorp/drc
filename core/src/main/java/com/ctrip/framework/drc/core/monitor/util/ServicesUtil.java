package com.ctrip.framework.drc.core.monitor.util;

import com.ctrip.framework.drc.core.driver.config.ConfigSource;
import com.ctrip.framework.drc.core.monitor.reporter.EventMonitor;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.ctrip.framework.drc.core.monitor.reporter.TransactionMonitor;
import com.ctrip.framework.drc.core.server.common.filter.service.UidService;

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

    public static UidService getUidService(){
        return load(UidService.class);
    }
}
