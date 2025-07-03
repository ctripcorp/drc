package com.ctrip.framework.drc.applier.utils;

import com.ctrip.framework.drc.core.monitor.enums.ConflictDetail;
import com.ctrip.xpipe.config.AbstractConfigBean;

import java.util.Set;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.PROCESSORS_SIZE;

/**
 * Created by jixinwang on 2023/4/7
 */
public class ApplierDynamicConfig extends AbstractConfigBean {

    private static final String APPLIER_LWM_TOLERANCE_TIME = "drc.applier.lwm.tolerance.time";
    private static final long DEFAULT_APPLIER_LWM_TOLERANCE_TIME = 60 * 1000; //1 minutes

    private static final String FIRST_APPLIER_LWM_TOLERANCE_TIME = "drc.first.applier.lwm.tolerance.time";
    private static final long DEFAULT_FIRST_APPLIER_LWM_TOLERANCE_TIME = 10 * 60 * 1000; //10 minutes
    
    private static final String CONFLICT_LOG_UPLOAD_URL = "conflict.log.upload.url";
    private static final String CONFLICT_LOG_UPLOAD_SWITCH = "conflict.log.upload.switch";
    private static final String CONFLICT_LOG_BRIEF_QUEUE_SIZE = "conflict.log.brief.queue.size";
    private static final String CONFLICT_LOG_BRIEF_REPORT_SIZE = "conflict.log.brief.report.size";
    private static final String CONFLICT_LOG_UPLOAD_LEVEL = "conflict.log.upload.level";
    private static final String DEFAULT_CONFLICT_LOG_UPLOAD_LEVEL = "INFO";
    private static final String APPLIER_INSTANCE_MODIFY_THREAD = "applier.instance.modify.thread";


    private ApplierDynamicConfig() {}
    
    private static class ConfigHolder {
        public static final ApplierDynamicConfig INSTANCE = new ApplierDynamicConfig();
    }

    public static ApplierDynamicConfig getInstance() {
        return ConfigHolder.INSTANCE;
    }
    
    public String getConflictLogUploadUrl() {
        return getProperty(CONFLICT_LOG_UPLOAD_URL, "");
    }
    
    public String getConflictLogUploadSwitch() {
        return getProperty(CONFLICT_LOG_UPLOAD_SWITCH, "off");
    }

    public int getBriefLogsQueueSize() {
        return getIntProperty(CONFLICT_LOG_BRIEF_QUEUE_SIZE, 40000);
    }

    public int getBriefLogsReportSize() {
        return getIntProperty(CONFLICT_LOG_BRIEF_REPORT_SIZE, 10000);
    }
    
    public long getLwmToleranceTime() {
        return getLongProperty(APPLIER_LWM_TOLERANCE_TIME, DEFAULT_APPLIER_LWM_TOLERANCE_TIME);
    }

    public long getFirstLwmToleranceTime() {
        return getLongProperty(FIRST_APPLIER_LWM_TOLERANCE_TIME, DEFAULT_FIRST_APPLIER_LWM_TOLERANCE_TIME);
    }

    public int getApplierInstanceModifyThread() {
        return getIntProperty(APPLIER_INSTANCE_MODIFY_THREAD, PROCESSORS_SIZE * 10);
    }

    public Set<ConflictDetail.AlertLevel> getConflictLogUpLevel() {
        String level = getProperty(CONFLICT_LOG_UPLOAD_LEVEL, DEFAULT_CONFLICT_LOG_UPLOAD_LEVEL);
        switch (level) {
            case "WARN":
                return Set.of(ConflictDetail.AlertLevel.WARN, ConflictDetail.AlertLevel.CRITICAL);
            case "CRITICAL":
                return Set.of(ConflictDetail.AlertLevel.CRITICAL);
            case "INFO":
            default:
                return Set.of(ConflictDetail.AlertLevel.INFO, ConflictDetail.AlertLevel.WARN, ConflictDetail.AlertLevel.CRITICAL);
        }
    }
}
