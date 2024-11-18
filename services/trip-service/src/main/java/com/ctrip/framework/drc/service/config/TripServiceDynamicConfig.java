package com.ctrip.framework.drc.service.config;

import com.ctrip.xpipe.config.AbstractConfigBean;

/**
 * Created by shiruixin
 * 2024/10/24 16:43
 */
public class TripServiceDynamicConfig extends AbstractConfigBean {
    private static String SUBENV_SWITCH = "subenv.switch";

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
}