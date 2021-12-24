package com.ctrip.framework.drc.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mingdongli
 * 2019/11/15 上午10:07.
 */
public abstract class AbstractStarter {

    protected static final Logger logger = LoggerFactory.getLogger(MonitorStarter.class);

    protected static String getPropertyOrDefault(String name, String defaultValue) {
        String value = System.getProperty(name);

        if (value == null) {
            value = System.getenv(name);
        }

        if (value == null) {
            return defaultValue;
        }
        return value;
    }
}
