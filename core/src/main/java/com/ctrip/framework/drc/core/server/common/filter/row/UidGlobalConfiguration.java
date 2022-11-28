package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.google.common.base.Preconditions;

import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.*;

/**
 * Created by jixinwang on 2022/11/28
 */
public class UidGlobalConfiguration extends AbstractConfigBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final static String UID_BLACKLIST_GLOBAL = "uid.filter.blacklist.global";

    private final static String localUidBlacklistPath = LOCAL_CONFIG_PATH + UID_BLACKLIST_GLOBAL;

    private final static String updateGlobalUidBlacklist = "update.uid.blacklist";

    private volatile Set<String> globalBlacklist = Sets.newHashSet();

    private static final int UPDATE_GLOBAL_BLACKLIST_TIME = 10;

    private ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("Update-Global-Uid-Blacklist-Task");

    private UidGlobalConfiguration() {
        updateBlacklist();
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                if ("true".equalsIgnoreCase(getProperty(updateGlobalUidBlacklist, "false"))) {
                    updateBlacklist();
                    logger.info("[GLOBAL][BLACKLIST] update global uid blacklist success");
                } else {
                    logger.info("[GLOBAL][BLACKLIST] update global uid blacklist ignore for update.uid.blacklist is: false");
                }
            } catch (Throwable t) {
                logger.error("[GLOBAL][BLACKLIST] update global uid blacklist error", t);
            }
        }, UPDATE_GLOBAL_BLACKLIST_TIME, UPDATE_GLOBAL_BLACKLIST_TIME, TimeUnit.MINUTES);
    }

    private static class  UidGlobalConfigurationHolder {
        public static final  UidGlobalConfiguration INSTANCE = new  UidGlobalConfiguration();
    }

    public static  UidGlobalConfiguration getInstance() {
        return  UidGlobalConfigurationHolder.INSTANCE;
    }

    private void updateBlacklist() {
        Set<String> localBlacklist = getLocalBlacklist();
        if (localBlacklist.isEmpty()) {
            globalBlacklist = getRemoteBlacklist();
        } else {
            globalBlacklist = localBlacklist;
        }
    }

    private Set<String> getLocalBlacklist() {
        Set<String> list = Sets.newHashSet();
        File localUidBlacklistFile = new File(localUidBlacklistPath);
        if (localUidBlacklistFile.exists()) {
            try {
                String blackUidStr = FileUtils.readFileToString(localUidBlacklistFile);
                if (blackUidStr != null) {
                    list = getSplitStringSet(blackUidStr.toLowerCase());
                }
            } catch (IOException e) {
                logger.error("[GLOBAL][BLACKLIST] get global blackList uid from file error", e);
            }
        }
        return list;
    }

    private Set<String> getRemoteBlacklist() {
        Set<String> list = Sets.newHashSet();
        String value = getProperty(UID_BLACKLIST_GLOBAL);
        if (StringUtils.isNotBlank(value)) {
            list = getSplitStringSet(value.toLowerCase());
            logger.info("[GLOBAL][BLACKLIST] get blacklist with size: {}", list.size());
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
}
