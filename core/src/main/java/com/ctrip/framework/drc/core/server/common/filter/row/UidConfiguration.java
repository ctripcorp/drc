package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.monitor.util.IsolateHashCache;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.ROWS_FILTER_LOGGER;

/**
 * @Author limingdong
 * @create 2022/6/30
 */
public class UidConfiguration extends AbstractConfigBean {

    private static final String UID_FILTER = "uid.filter.";

    private static final String WHITELIST = "whitelist.";

    private static final String BLACKLIST = "blacklist.";

    protected static final String UID_WHITELIST = UID_FILTER + WHITELIST + "%s";

    protected static final String UID_BLACKLIST = UID_FILTER + BLACKLIST + "%s";

    private IsolateHashCache<String, Set<String>> blackListCache = new IsolateHashCache<>(5000, 16, 4);

    private IsolateHashCache<String, Set<String>> whiteListCache = new IsolateHashCache<>(5000, 16, 4);

    private UidConfiguration() {}

    private static class UidConfigurationHolder {
        public static final UidConfiguration INSTANCE = new UidConfiguration();
    }

    public static UidConfiguration getInstance() {
        return UidConfigurationHolder.INSTANCE;
    }

    public boolean filterRowsWithBlackList(UserContext uidContext) throws Exception {
        try {
            String registryKey = uidContext.getRegistryKey();
            String uid = uidContext.getUserAttr();
            Preconditions.checkArgument(StringUtils.isNotBlank(uid));
            Set<String> blackListUids = blackListCache.get(registryKey, () -> getList(String.format(UID_BLACKLIST, registryKey)));
            boolean res = !blackListUids.contains(uid.trim().toLowerCase());
            if (!res) {
                ROWS_FILTER_LOGGER.info("[Filter] one row of uid {} for {} due to black list", uid, registryKey);
            }
            return res;
        } catch (IllegalArgumentException e) {
            return uidContext.getIllegalArgument();
        }
    }

    public boolean filterRowsWithWhiteList(UserContext uidContext) throws Exception {
        try {
            String registryKey = uidContext.getRegistryKey();
            String uid = uidContext.getUserAttr();
            Preconditions.checkArgument(StringUtils.isNotBlank(uid));
            Set<String> whiteListUids = whiteListCache.get(registryKey, () -> getList(String.format(UID_WHITELIST, registryKey)));
            return whiteListUids.contains(uid.trim().toLowerCase());
        } catch (IllegalArgumentException e) {
            return uidContext.getIllegalArgument();
        }
    }

    private Set<String> getList(String key) {
        String value = getProperty(key);
        if (StringUtils.isNotBlank(value)) {
            return getSplitStringSet(value.toLowerCase());
        }
        return Sets.newHashSet();
    }

    @Override
    public void onChange(String key, String oldValue, String newValue) {
        super.onChange(key, oldValue, newValue);
        logger.debug("[onChange] for key {}:{}:{}", key, oldValue, newValue);
        if (key != null && key.startsWith(UID_FILTER)) {
            String registryKey = key.substring(UID_FILTER.length());
            if (registryKey.startsWith(BLACKLIST)) {
                registryKey = registryKey.substring(BLACKLIST.length());
                blackListCache.invalidate(registryKey);
                logger.info("[Invalidate] blackListCache for key {}", registryKey);
            } else if (registryKey.startsWith(WHITELIST)) {
                registryKey = registryKey.substring(WHITELIST.length());
                whiteListCache.invalidate(registryKey);
                logger.info("[Invalidate] whiteListCache for key {}", registryKey);
            }
        }
    }

    @VisibleForTesting
    protected IsolateHashCache<String, Set<String>> getBlackListCache() {
        return blackListCache;
    }

    @VisibleForTesting
    protected IsolateHashCache<String, Set<String>> getWhiteListCache() {
        return whiteListCache;
    }

    @VisibleForTesting
    protected void clear() {
        blackListCache.invalidateAll();
        whiteListCache.invalidateAll();
    }
}
