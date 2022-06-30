package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.xpipe.config.AbstractConfigBean;

import java.util.Set;

/**
 * @Author limingdong
 * @create 2022/6/30
 */
public class UidConfiguration extends AbstractConfigBean {

    private static final String UID_FILTER = "uid.filter";

    private static final String UID_WHITELIST = UID_FILTER + ".whitelist.%s";

    private static final String UID_BLACKLIST = UID_FILTER + ".blacklist.%s";

    private UidConfiguration() {}

    private static class UidConfigurationHolder {
        public static final UidConfiguration INSTANCE = new UidConfiguration();
    }

    public static UidConfiguration getInstance() {
        return UidConfigurationHolder.INSTANCE;
    }

    public boolean filterRowsWithBlackList(String uid, String registryKey) {
        String blackList = String.format(UID_BLACKLIST, registryKey);
        Set<String> blackListUids = getSplitStringSet(blackList);
        if (blackListUids.contains(uid.trim())) {
            return false;
        }
        return true;
    }

    public boolean filterRowsWithWhiteList(String uid, String registryKey) {
        String whiteList = String.format(UID_WHITELIST, registryKey);
        Set<String> whiteListUids = getSplitStringSet(whiteList);
        if (whiteListUids.contains(uid.trim())) {
            return true;
        }
        return false;
    }

    @Override
    public void onChange(String key, String oldValue, String newValue) {
        super.onChange(key, oldValue, newValue);
        logger.debug("[onChange] for key {}:{}:{}", key, oldValue, newValue);
        if (key != null && key.startsWith(UID_FILTER)) {

        }
    }

}