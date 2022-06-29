package com.ctrip.framework.drc.fetcher.event.config;

import com.ctrip.xpipe.config.AbstractConfigBean;

/**
 * @Author limingdong
 * @create 2021/2/7
 */
public class BigTransactionThreshold extends AbstractConfigBean {

    public static final String BIG_TRANSACTION_THRESHOLD = "big.transaction";

    public static final int CAPACITY = 2;

    private static class BigTransactionThresholdHolder {
        public static final BigTransactionThreshold INSTANCE = new BigTransactionThreshold();
    }

    public static BigTransactionThreshold getInstance() {
        return BigTransactionThresholdHolder.INSTANCE;
    }

    public int getThreshold() {
        return getIntProperty(BIG_TRANSACTION_THRESHOLD, CAPACITY);
    }
}
