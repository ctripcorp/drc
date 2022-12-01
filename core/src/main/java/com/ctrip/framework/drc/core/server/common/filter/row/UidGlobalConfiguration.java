package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.*;

/**
 * Created by jixinwang on 2022/11/28
 */
public class UidGlobalConfiguration extends AbstractConfigBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final static String UID_BLACKLIST_GLOBAL = "uid.filter.blacklist.global";

    private File localUidBlacklistFile = new File(LOCAL_CONFIG_PATH + UID_BLACKLIST_GLOBAL);

    private volatile Set<String> globalBlacklist = Sets.newHashSet();

    private UidGlobalConfiguration() {}

    private static class  UidGlobalConfigurationHolder {
        public static final  UidGlobalConfiguration INSTANCE = new  UidGlobalConfiguration();
    }

    public static  UidGlobalConfiguration getInstance() {
        return  UidGlobalConfigurationHolder.INSTANCE;
    }

    @VisibleForTesting
    protected void updateBlacklist() {
        Set<String> localBlacklist = getLocalBlacklist();
        if (localBlacklist.isEmpty()) {
            globalBlacklist = getRemoteBlacklist();
        } else {
            globalBlacklist = localBlacklist;
        }
        logger.info("[GLOBAL][BLACKLIST] update global uid blacklist success");
    }

    private Set<String> getLocalBlacklist() {
        Set<String> list = Sets.newHashSet();
        if (localUidBlacklistFile.exists()) {
            try {
                String blacklistStr = FileUtils.readFileToString(localUidBlacklistFile);
                if (blacklistStr != null) {
                    list = getSplitStringSet(blacklistStr.trim().toLowerCase());
                    logger.info("[GLOBAL][BLACKLIST] get local blacklist with size: {}", list.size());
                }
            } catch (IOException e) {
                logger.error("[GLOBAL][BLACKLIST] get local global blackList uid error", e);
            }
        }
        return list;
    }

    private Set<String> getRemoteBlacklist() {
        Set<String> list = Sets.newHashSet();
        String value = getProperty(UID_BLACKLIST_GLOBAL);
        if (StringUtils.isNotBlank(value)) {
            try {
                list = getSplitStringSet(value.trim().toLowerCase());
                logger.info("[GLOBAL][BLACKLIST] get blacklist with size: {}", list.size());
            } catch (Exception e) {
                logger.error("[GLOBAL][BLACKLIST] get remote global blackList uid error", e);
            }
        }
        return list;
    }

    public boolean filterRowsWithBlackList(UserContext uidContext) throws Exception {
        try {
            String registryKey = uidContext.getRegistryKey();
            String uid = uidContext.getUserAttr();
            Preconditions.checkArgument(StringUtils.isNotBlank(uid));
            boolean res = !globalBlacklist.contains(uid.trim().toLowerCase());
            if (!res) {
                ROWS_FILTER_LOGGER.info("[Filter] one row of uid {} for {} due to global black list", uid, registryKey);
            }
            return res;
        } catch (IllegalArgumentException e) {
            return uidContext.getIllegalArgument();
        }
    }

    @Override
    public void onChange(String key, String oldValue, String newValue) {
        super.onChange(key, oldValue, newValue);
        if (UID_BLACKLIST_GLOBAL.equalsIgnoreCase(key)) {
            updateBlacklist();
        }
    }

    @VisibleForTesting
    protected void setFile(File file) {
        this.localUidBlacklistFile = file;
    }
}
