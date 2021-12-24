package com.ctrip.framework.drc.fetcher.system.qconfig;

import com.ctrip.framework.drc.core.monitor.util.ServicesUtil;
import com.ctrip.framework.drc.fetcher.system.AbstractUnit;
import com.ctrip.framework.drc.core.driver.config.ConfigSource;

import java.lang.reflect.Field;

/**
 * @Author Slight
 * Jun 16, 2020
 */
public class AbstractUnitUsingQConfig extends AbstractUnit {

    protected ConfigSource source = ServicesUtil.getConfigSourceService();

    @Override
    protected void loadConfig(Object object, Field field, String path) throws Exception {
        super.loadConfig(object, field, path);
        String namespace = namespace();
        ConfigKey key = ConfigKey.from(namespace + ".properties", path);
        if (namespace != null) {
            try {
                source.load(object, field, key);
                source.bind(object, field, key);
                logger.info("bind qconfig ({}): {}/{}", namespace, key.filename, key.key);
            } catch (Throwable t) {
                logger.debug("fail to bind qconfig ({}): {}/{}", namespace, key.filename, key.key ,t);
            }
        }
    }
}

