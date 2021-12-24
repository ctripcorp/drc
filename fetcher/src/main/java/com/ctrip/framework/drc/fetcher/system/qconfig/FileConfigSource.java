package com.ctrip.framework.drc.fetcher.system.qconfig;

import com.ctrip.framework.drc.core.driver.config.ConfigSource;
import com.ctrip.framework.drc.core.server.config.DefaultFileConfig;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author limingdong
 * @create 2021/12/2
 */
public class FileConfigSource extends AbstractConfigSource implements ConfigSource<ConfigKey> {

    private Map<String, DefaultFileConfig> configMap = new ConcurrentHashMap<>();

    @Override
    public Object get(ConfigKey key) {
        DefaultFileConfig fileConfig = configMap.get(key.filename);
        if (fileConfig == null) {
            fileConfig = new DefaultFileConfig(key.filename);
            configMap.put(key.key, fileConfig);
        }
        return fileConfig.get(key.key);
    }

    @Override
    public void bind(Object object, Field field, ConfigKey key) {

    }

    @Override
    public int getOrder() {
        return 1;
    }

}
