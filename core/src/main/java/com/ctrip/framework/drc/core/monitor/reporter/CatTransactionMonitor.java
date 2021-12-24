package com.ctrip.framework.drc.core.monitor.reporter;

/**
 * @Author limingdong
 * @create 2021/11/30
 */
public class CatTransactionMonitor extends com.ctrip.xpipe.monitor.CatTransactionMonitor implements TransactionMonitor {

    @Override
    public int getOrder() {
        return 0;
    }
}
