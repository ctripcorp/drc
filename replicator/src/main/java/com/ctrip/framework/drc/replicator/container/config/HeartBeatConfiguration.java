package com.ctrip.framework.drc.replicator.container.config;

import com.ctrip.xpipe.config.AbstractConfigBean;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * @Author limingdong
 * @create 2022/3/29
 */
public class HeartBeatConfiguration extends AbstractConfigBean {

    private static class HeartBeatConfigurationHolder {
        public static final HeartBeatConfiguration INSTANCE = new HeartBeatConfiguration();
    }

    public static HeartBeatConfiguration getInstance() {
        return HeartBeatConfigurationHolder.INSTANCE;
    }

    public static final String HEARTBEAT_SWITCH = "heartbeat.switch";

    public static final String HEARTBEAT_GRAY_KEY = "heartbeat.switch.gray";

    public boolean gray(String registerKey) {
        if (!getHeartBeatSwitch()) {
            return false;
        }
        Set<String> keys = getGrayHeartBeat();
        if (keys == null || keys.isEmpty()) {
            return false;
        }
        if (keys.contains("*")) {
            return true;
        }
        return keys.contains(registerKey);
    }

    private Set<String> getGrayHeartBeat() {
        String grayCluster = getProperty(HEARTBEAT_GRAY_KEY, "");
        return Sets.newHashSet(grayCluster.split(","));
    }

    public boolean getHeartBeatSwitch() {
        return getBooleanProperty(HEARTBEAT_SWITCH, true);
    }
}