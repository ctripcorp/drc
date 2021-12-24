package com.ctrip.framework.drc.core.monitor.reporter;

import com.ctrip.framework.drc.core.monitor.util.ServicesUtil;

/**
 * @Author limingdong
 * @create 2021/11/30
 */
public class DefaultTransactionMonitorHolder {

    private static class TransactionMonitorHolder {
        public static final TransactionMonitor INSTANCE = ServicesUtil.getTransactionMonitorService();
    }

    public static TransactionMonitor getInstance() {
        return TransactionMonitorHolder.INSTANCE;
    }
}
