package com.ctrip.framework.drc.fetcher.system.qconfig;

import com.ctrip.xpipe.config.AbstractConfigBean;

/**
 * Created by shiruixin
 * 2024/10/28 17:54
 */
public class FetcherDynamicConfig extends AbstractConfigBean {
    private static final String MQ_APPLY_COUNT = "mq.apply.count";
    public static final int DEFAULT_MQ_APPLY_COUNT = 50;

    private static final String APPLIER_LWM_TOLERANCE_TIME = "drc.applier.lwm.tolerance.time";
    private static final long DEFAULT_APPLIER_LWM_TOLERANCE_TIME = 60 * 1000; //1 minutes

    private static final String FIRST_APPLIER_LWM_TOLERANCE_TIME = "drc.first.applier.lwm.tolerance.time";
    private static final long DEFAULT_FIRST_APPLIER_LWM_TOLERANCE_TIME = 10 * 60 * 1000; //10 minutes

    private static final String MESSENGER_APPLY_COUNT_PATTERN = "mq.apply.count.%s";

    private FetcherDynamicConfig() {}

    private static class ConfigHolder {
        public static final FetcherDynamicConfig INSTANCE = new FetcherDynamicConfig();
    }

    public static FetcherDynamicConfig getInstance() {
        return ConfigHolder.INSTANCE;
    }

    public int getMqApplyCount(String registryKey) {
        int defaultMqApplyCount = getIntProperty(MQ_APPLY_COUNT, DEFAULT_MQ_APPLY_COUNT);
        return getIntProperty(String.format(MESSENGER_APPLY_COUNT_PATTERN, registryKey), defaultMqApplyCount);
    }

    public long getLwmToleranceTime() {
        return getLongProperty(APPLIER_LWM_TOLERANCE_TIME, DEFAULT_APPLIER_LWM_TOLERANCE_TIME);
    }

    public long getFirstLwmToleranceTime() {
        return getLongProperty(FIRST_APPLIER_LWM_TOLERANCE_TIME, DEFAULT_FIRST_APPLIER_LWM_TOLERANCE_TIME);
    }



}
