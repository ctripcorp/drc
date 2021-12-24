package com.ctrip.framework.drc.service.config;

import com.ctrip.xpipe.api.config.ConfigChangeListener;
import com.ctrip.xpipe.config.AbstractConfig;
import qunar.tc.qconfig.client.Configuration;
import qunar.tc.qconfig.client.MapConfig;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DEFAULT_CONFIG_FILE_NAME;

/**
 * Created by mingdongli
 * 2019/10/31 下午2:25.
 */
public class QConfig extends AbstractConfig implements ConfigChangeListener {

    private MapConfig config = MapConfig.get(DEFAULT_CONFIG_FILE_NAME);

    private Map<String, String> currentConfigs = new HashMap<>(config.asMap());

    public QConfig() {
        config.asMap();
        config.addListener(new Configuration.ConfigListener<>() {
            @Override
            public synchronized void onLoad(Map<String, String> conf) {
                for (Map.Entry<String, String> entry : conf.entrySet()) {
                    String key = entry.getKey();
                    String currentValue = entry.getValue();
                    String oldValue = currentConfigs.get(key);
                    if (currentValue != null && !currentValue.equalsIgnoreCase(oldValue)) { // add or update
                        onChange(key, oldValue, currentValue);
                    }
                }

                Set<String> deleted = new HashSet<>(currentConfigs.keySet());
                deleted.removeAll(conf.keySet());

                for (String key : deleted) {  // delete
                    onChange(key, currentConfigs.get(key), null);
                }

                currentConfigs.clear();
                currentConfigs.putAll(conf);
            }
        });
    }

    @Override
    public String get(String key) {
        return config.asMap().get(key);
    }

    @Override
    public String get(String key, String defaultValue) {
        return config.asMap().getOrDefault(key, defaultValue);
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }

    @Override
    public void onChange(String key, String oldValue, String newValue) {
        notifyConfigChange(key, oldValue, newValue);
    }
}
