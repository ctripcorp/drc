package com.ctrip.framework.drc.service.config;

import com.ctrip.framework.drc.core.driver.config.ConfigSource;
import com.ctrip.framework.drc.fetcher.system.qconfig.AbstractConfigSource;
import com.ctrip.framework.drc.fetcher.system.qconfig.ConfigKey;
import com.google.common.collect.Sets;
import qunar.tc.qconfig.client.Configuration;
import qunar.tc.qconfig.client.MapConfig;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

/**
 * @Author Slight
 * Jun 16, 2020
 */
public class QConfigSource extends AbstractConfigSource implements ConfigSource<ConfigKey> {

    private static Set<String> listenerSet = Sets.newConcurrentHashSet();

    @Override
    public Object get(ConfigKey key) {
        return MapConfig.get(key.filename).asMap().get(key.key);
    }

    @Override
    public void bind(Object object, Field field, ConfigKey key) {
        if (!listenerSet.contains(key.filename + '.' + key.key)) {
            MapConfig.get(key.filename).addListener(new Configuration.ConfigListener<Map<String, String>>() {
                @Override
                public void onLoad(Map<String, String> conf) {
                    String value = conf.get(key.key);
                    if (value == null) {
                        return;
                    }
                    try {
                        Class type = field.getType();

                        if (type == long.class || type == Long.class) {
                            field.set(object, Long.parseLong(value));
                        }

                        if (type == int.class || type == Integer.class) {
                            field.set(object, Integer.parseInt(value));
                        }

                        if (type == String.class) {
                            field.set(object, value);
                        }

                        logger.info("config changed - {}/{}: {}", key.filename, key.key, value);
                    } catch (Throwable t) {
                        logger.error("when {}/{} notified.", key.filename, key.key, t);
                    }
                }
            });
            listenerSet.add(key.filename + '.' + key.key);
        }
    }

    public static Set<String> getListenerSet() {
        return Sets.newHashSet(listenerSet);
    }
}
