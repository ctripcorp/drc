package com.ctrip.framework.drc.console.config;

import com.ctrip.xpipe.config.AbstractConfigBean;
import com.google.common.collect.Sets;
import org.springframework.context.annotation.Configuration;


import java.util.Set;

@Configuration
public class UdlMigrateConfiguration extends AbstractConfigBean {
    
    private static final String UDL_MIGRATE_SWITCH = "udl.migrate.switch";
    private static final String UDL_MIGRATE_GRAY = "udl.migrate.gray";
    
    public boolean gray(Long applierGroupId) {
        if (!getUdlMigrateSwitch()) {
            return false;
        }
        Set<String> udlGraySet = getUdlGraySet();
        if (udlGraySet.isEmpty()) {
            return false;
        }
        if (udlGraySet.contains("*")) {
            return true;
        }
        return udlGraySet.contains(String.valueOf(applierGroupId));
    }
    
    /**
     * @return Set of applierGroupId which would be gray migrated
     */
    private Set<String> getUdlGraySet() {
        // applierGroupIdSet
        String idString = getProperty(UDL_MIGRATE_GRAY, "");
        return Sets.newHashSet(idString.split(","));
    }
    
    private boolean getUdlMigrateSwitch() {
        return getBooleanProperty(UDL_MIGRATE_SWITCH,false);
    }
}
