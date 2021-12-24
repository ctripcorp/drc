package com.ctrip.framework.drc.replicator.container.config;

import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.BlackTableNameFilter;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @Author limingdong
 * @create 2021/8/3
 */
@Component
public class TableFilterConfiguration extends AbstractConfigBean {

    private Logger logger = LoggerFactory.getLogger(getClass());

    public static final String FILTER_TABLES = "filtered.tables";

    private Map<String, BlackTableNameFilter> watched = Maps.newConcurrentMap();

    public void register(String key, BlackTableNameFilter filter) {
        Collection<String> filteredTables = getFilteredTables(key);
        update(key, filter, filteredTables);
        watched.put(key, filter);
        logger.info("[Register] filtered talbes -> {}:{}", key, filter.getEXCLUDED_TABLE());
    }

    public BlackTableNameFilter unregister(String key) {
        logger.info("[Unregister] filtered talbes for {}", key);
        return watched.remove(key);
    }

    private Collection<String> getFilteredTables(String registryKey) {
        String watchKey = getClusterKey(registryKey);
        String filteredTables = getProperty(watchKey);
        if (filteredTables == null) {
            filteredTables = getProperty(FILTER_TABLES);
        }

        return split(filteredTables);
    }

    private Collection<String> split(String value) {
        if (value == null) {  // delete
            value = getProperty(FILTER_TABLES);
        }
        if (StringUtils.isBlank(value)) {
            return Sets.newHashSet();
        }
        String[] tables = value.split(SystemConfig.COMMA);
        return Sets.newHashSet(tables);
    }

    private String getClusterKey(String registryKey) {
        return FILTER_TABLES + SystemConfig.DOT + registryKey;
    }

    @Override
    public void onChange(String key, String oldValue, String newValue) {
        super.onChange(key, oldValue, newValue);
        logger.debug("[onChange] for key {}:{}:{}", key, oldValue, newValue);
        if (key != null && key.startsWith(FILTER_TABLES)) {
            if (FILTER_TABLES.equalsIgnoreCase(key)) {
                Collection<String> filteredTables = split(newValue);
                for (Map.Entry<String, BlackTableNameFilter> entry : watched.entrySet()) {
                    String watchedKey = getClusterKey(entry.getKey());
                    if (getProperty(watchedKey) == null) {
                        update(watchedKey, entry.getValue(), filteredTables);
                    }
                }
                return;
            }

            for (Map.Entry<String, BlackTableNameFilter> entry : watched.entrySet()) {
                String watchedKey = getClusterKey(entry.getKey());
                if (watchedKey.equalsIgnoreCase(key)) {
                    Collection<String> filteredTables = split(newValue);
                    update(key, entry.getValue(), filteredTables);
                    return;
                }
            }
        }
    }

    private void update(String key, BlackTableNameFilter filter, Collection<String> filteredTables) {

        Set<String> origin = filter.getEXCLUDED_TABLE();
        // get deleted
        Set<String> deleted = new HashSet<>(origin);
        deleted.removeAll(filteredTables);

        origin.removeAll(deleted);
        origin.addAll(filteredTables);
        logger.info("[Update] filtered talbes -> {}:{}", key, filteredTables);
    }
}