package com.ctrip.framework.drc.fetcher.system.qconfig;

import com.ctrip.framework.drc.core.driver.config.ConfigSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

/**
 * @Author limingdong
 * @create 2021/12/2
 */
public abstract class AbstractConfigSource implements ConfigSource<ConfigKey> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void load(Object object, Field field, ConfigKey key) {
        String value = (String) get(key);
        if (value == null) {
            return;
        }
        try {
            Class type = field.getType();

            if (type == long.class || type == Long.class){
                field.set(object, Long.parseLong(value));
            }

            if (type == int.class || type == Integer.class) {
                field.set(object, Integer.parseInt(value));
            }

            if (type == String.class){
                field.set(object, value);
            }

            logger.info("config loaded - {}/{}: {}", key.filename, key.key, value);
        } catch (Throwable t) {
            logger.error("when loading {}/{}.", key.filename, key.key, t);
        }
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
