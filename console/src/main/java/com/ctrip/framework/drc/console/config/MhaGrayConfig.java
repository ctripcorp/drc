package com.ctrip.framework.drc.console.config;

import com.ctrip.xpipe.config.AbstractConfigBean;
import com.google.common.collect.Sets;
import org.springframework.context.annotation.Configuration;

import java.util.Set;

/**
 * @ClassName MhaGrayConfig
 * @Author haodongPan
 * @Date 2022/10/13 19:59
 * @Version: $
 */
@Configuration
public class MhaGrayConfig extends AbstractConfigBean{

    private static final String MHA_GRAY_SWITCH = "mha.gray.switch";
    private static final String MHA_MIGRATE_GRAY = "mha.migrate.gray";

    private static final String DBCLUSTER_GRAY_SWITCH = "dbCluster.gray.switch";
    private static final String DBCLUSTER_MIGRATE_GRAY = "dbCluster.migrate.gray";
    private static final String DBCLUSTER_GRAY_COMPARE_SWITCH = "dbCluster.gray.compare.switch";
    private static final String DBCLUSTER_MIGRATE_GRAY_ALL = "*";

    public Set<String> getGrayDbClusterSet() {
        String dbClusterSetString = getProperty(DBCLUSTER_MIGRATE_GRAY, null);
        if (dbClusterSetString == null) {
            return Sets.newHashSet();
        } else {
            return Sets.newHashSet(dbClusterSetString.split(","));
        }
    }
    
    public boolean grayAllDbCluster() {
        Set<String> dbClusterSet = getGrayDbClusterSet();
        return dbClusterSet.contains(DBCLUSTER_MIGRATE_GRAY_ALL);
    }
    

    public boolean getDbClusterGraySwitch() {
        return getBooleanProperty(DBCLUSTER_GRAY_SWITCH,false);
    }
    public boolean getDbClusterGrayCompareSwitch() {
        return getBooleanProperty(DBCLUSTER_GRAY_COMPARE_SWITCH,true);
    }
    
    
    public boolean gray(String mhaName) {
        if (!getMhaMigrateSwitch()) {
            return false;
        }
        Set<String> mhaGraySet = getMhaGraySet();
        if (mhaGraySet.isEmpty()) {
            return false;
        }
        if (mhaGraySet.contains("*")) {
            return true;
        }
        return mhaGraySet.contains(mhaName);
    }

    /**
     * @return Set of mha which would be gray migrated
     */
    private Set<String> getMhaGraySet() {
        // mhasStringSet
        String mhasStringSet = getProperty(MHA_MIGRATE_GRAY, "");
        return Sets.newHashSet(mhasStringSet.split(","));
    }

    private boolean getMhaMigrateSwitch() {
        return getBooleanProperty(MHA_GRAY_SWITCH,false);
    }
}