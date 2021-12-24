package com.ctrip.framework.drc.core.monitor.reporter;

import com.ctrip.framework.drc.core.monitor.util.ServicesUtil;

/**
 * @Author limingdong
 * @create 2021/10/25
 */
public class DefaultReporterHolder {

    private static class ReporterHolder {
        public static final Reporter INSTANCE = ServicesUtil.getReportService();
    }

    public static Reporter getInstance() {
        return ReporterHolder.INSTANCE;
    }
}
