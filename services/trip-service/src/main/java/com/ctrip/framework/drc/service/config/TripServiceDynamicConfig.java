package com.ctrip.framework.drc.service.config;

import com.ctrip.xpipe.config.AbstractConfigBean;

/**
 * Created by shiruixin
 * 2024/10/24 16:43
 */
public class TripServiceDynamicConfig extends AbstractConfigBean {
    private static final String SUBENV_SWITCH = "subenv.switch";
    private static final String KAFKA_APPID_TOKEN = "kafka.appid.token";
    private static final String CPU_OPTIMIZE_SWITCH = "cpu.optimize.switch";
    private static final String TOPIC_CPU_OPTIMIZE_SWITCH = "cpu.optimize.switch.%s";
    private static final String CPU_OPTIMIZE_COMPARE_SWITCH = "cpu.optimize.compare.switch";
    private static final String CPU_OPTIMIZE_COMPARE_SWITCH_PATTERN = "cpu.optimize.compare.switch.%s";

    private TripServiceDynamicConfig() {}

    private static class TripServiceDynamicConfigHolder {
        public static final TripServiceDynamicConfig INSTANCE = new TripServiceDynamicConfig();
    }

    public static TripServiceDynamicConfig getInstance() {
        return TripServiceDynamicConfigHolder.INSTANCE;
    }

    public boolean isSubenvEnable() {
        String subenvSwitch = getProperty(SUBENV_SWITCH, "off");
        return subenvSwitch.equalsIgnoreCase("on");
    }

    public String getKafkaAppidToken(){
        return getProperty(KAFKA_APPID_TOKEN, "");
    }

    public boolean isCpuOptimizeEnable(String topic) {
        String defaultSwitch = getProperty(CPU_OPTIMIZE_SWITCH, "off");
        String cpuOptimizeSwitch = getProperty(String.format(TOPIC_CPU_OPTIMIZE_SWITCH, topic), defaultSwitch);
        return cpuOptimizeSwitch.equalsIgnoreCase("on");
    }

    public boolean isCpuOptimizeCompareModeEnable(String topic) {
        String defaultSwitch = getProperty(CPU_OPTIMIZE_COMPARE_SWITCH, "off");
        String compareSwitch = getProperty(String.format(CPU_OPTIMIZE_COMPARE_SWITCH_PATTERN, topic), defaultSwitch);
        return compareSwitch.equalsIgnoreCase("on");
    }
}