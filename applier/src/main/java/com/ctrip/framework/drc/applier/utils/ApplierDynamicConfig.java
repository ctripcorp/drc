package com.ctrip.framework.drc.applier.utils;

import com.ctrip.xpipe.config.AbstractConfigBean;

/**
 * Created by jixinwang on 2023/4/7
 */
public class ApplierDynamicConfig extends AbstractConfigBean {

    private static final String APPLIER_LWM_TOLERANCE_TIME = "drc.applier.lwm.tolerance.time";
    private static final long DEFAULT_APPLIER_LWM_TOLERANCE_TIME = 60 * 1000; //1 minutes

    private static final String FIRST_APPLIER_LWM_TOLERANCE_TIME = "drc.first.applier.lwm.tolerance.time";
    private static final long DEFAULT_FIRST_APPLIER_LWM_TOLERANCE_TIME = 10 * 60 * 1000; //10 minutes

    private ApplierDynamicConfig() {}

    private static class ConfigHolder {
        public static final ApplierDynamicConfig INSTANCE = new ApplierDynamicConfig();
    }

    public static ApplierDynamicConfig getInstance() {
        return ConfigHolder.INSTANCE;
    }
    
    public String getConflictLogUploadUrl() {
        return getProperty("conflict.log.upload.url", "");
    }
    
    public String getConflictLogUploadSwitch() {
        return getProperty("conflict.log.upload.switch", "off");
    }


    public long getLwmToleranceTime() {
        return getLongProperty(APPLIER_LWM_TOLERANCE_TIME, DEFAULT_APPLIER_LWM_TOLERANCE_TIME);
    }

    public long getFirstLwmToleranceTime() {
        return getLongProperty(FIRST_APPLIER_LWM_TOLERANCE_TIME, DEFAULT_FIRST_APPLIER_LWM_TOLERANCE_TIME);
    }
}
