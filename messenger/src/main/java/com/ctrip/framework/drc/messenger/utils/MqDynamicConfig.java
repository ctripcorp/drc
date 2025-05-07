package com.ctrip.framework.drc.messenger.utils;

import com.ctrip.xpipe.config.AbstractConfigBean;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.PROCESSORS_SIZE;

/**
 * Created by jixinwang on 2023/4/7
 */
public class MqDynamicConfig extends AbstractConfigBean {

    private static final String APPLIER_LWM_TOLERANCE_TIME = "drc.applier.lwm.tolerance.time";
    private static final long DEFAULT_APPLIER_LWM_TOLERANCE_TIME = 60 * 1000; //1 minutes

    private static final String FIRST_APPLIER_LWM_TOLERANCE_TIME = "drc.first.applier.lwm.tolerance.time";
    private static final long DEFAULT_FIRST_APPLIER_LWM_TOLERANCE_TIME = 10 * 60 * 1000; //10 minutes

    private static final String APPLIER_INSTANCE_MODIFY_THREAD = "applier.instance.modify.thread";
    private static final String MESSENGER_INSTANCE_MODIFY_THREAD = "messenger.instance.modify.thread";

    private static final String MESSENGER_BIG_ROWSEVENT_SIZE = "messenger.big.rowsevent.size";
    private static final int DEFAULT_QMQ_BIG_ROWSEVENT_SIZE = 20;

    private static final String MESSENGER_ROWEVENT_EXECUTOR_MAXTHREAD = "mq.rowsexecutor.maxthread";
    private static final int DEFAULT_MESSENGER_ROWEVENT_EXECUTOR_MAXTHREAD = 50;
    private static final String ROWEVENT_EXECUTOR_PATTERN = "mq.rowsexecutor.maxthread.%s";

    private MqDynamicConfig() {}
    
    private static class ConfigHolder {
        public static final MqDynamicConfig INSTANCE = new MqDynamicConfig();
    }

    public static MqDynamicConfig getInstance() {
        return ConfigHolder.INSTANCE;
    }
    
    public long getLwmToleranceTime() {
        return getLongProperty(APPLIER_LWM_TOLERANCE_TIME, DEFAULT_APPLIER_LWM_TOLERANCE_TIME);
    }

    public long getFirstLwmToleranceTime() {
        return getLongProperty(FIRST_APPLIER_LWM_TOLERANCE_TIME, DEFAULT_FIRST_APPLIER_LWM_TOLERANCE_TIME);
    }

    public int getMessengerInstanceModifyThread() {
        return getIntProperty(MESSENGER_INSTANCE_MODIFY_THREAD, PROCESSORS_SIZE * 5);
    }

    public int getBigRowsEventSize() {
        return getIntProperty(MESSENGER_BIG_ROWSEVENT_SIZE, DEFAULT_QMQ_BIG_ROWSEVENT_SIZE);
    }

    public int getRowEventExecutorMaxThread(String registryKey) {
        int defaultMaxThread = getIntProperty(MESSENGER_ROWEVENT_EXECUTOR_MAXTHREAD, DEFAULT_MESSENGER_ROWEVENT_EXECUTOR_MAXTHREAD);
        return getIntProperty(String.format(ROWEVENT_EXECUTOR_PATTERN, registryKey), defaultMaxThread);
    }

}
