package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.monitor.util.IsolateHashCache;
import com.ctrip.xpipe.config.AbstractConfigBean;

import java.util.Set;

/**
 * @Author limingdong
 * @create 2022/6/30
 */
public class UidConfiguration extends AbstractConfigBean {

    private static final String UID_FILTER = "uid.filter.";

    private static final String UID_WHITELIST = UID_FILTER + "whitelist.%s";

    private static final String UID_BLACKLIST = UID_FILTER + "blacklist.%s";

    private IsolateHashCache<String, Set<String>> blackListCache = new IsolateHashCache<>(5000, 16, 4);

    private IsolateHashCache<String, Set<String>> whiteListCache = new IsolateHashCache<>(5000, 16, 4);

    private UidConfiguration() {}

    private static class UidConfigurationHolder {
        public static final UidConfiguration INSTANCE = new UidConfiguration();
    }

    public static UidConfiguration getInstance() {
        return UidConfigurationHolder.INSTANCE;
    }

    public boolean filterRowsWithBlackList(String uid, String registryKey) throws Exception {
        Set<String> blackListUids = blackListCache.get(registryKey, () -> {
            String blackList = String.format(UID_BLACKLIST, registryKey);
            return getSplitStringSet(blackList);
        });
        return !blackListUids.contains(uid.trim());
    }

    public boolean filterRowsWithWhiteList(String uid, String registryKey) throws Exception {
        Set<String> whiteListUids = whiteListCache.get(registryKey, () -> {
            String whiteList = String.format(UID_WHITELIST, registryKey);
            return getSplitStringSet(whiteList);
        });
        return whiteListUids.contains(uid.trim());
    }

    @Override
    public void onChange(String key, String oldValue, String newValue) {
        super.onChange(key, oldValue, newValue);
        logger.debug("[onChange] for key {}:{}:{}", key, oldValue, newValue);
        if (key != null && key.startsWith(UID_FILTER)) {
            String registryKey = key.substring(UID_FILTER.length());
            blackListCache.invalidate(registryKey);
            whiteListCache.invalidate(registryKey);
            logger.info("[Invalidate] for key {}", registryKey);
        }
    }

}