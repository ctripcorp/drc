package com.ctrip.framework.drc.core.config;

import com.ctrip.xpipe.config.AbstractConfigBean;

import static com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager.MAX_ACTIVE;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.SOCKET_TIMEOUT;

/**
 * Created by jixinwang on 2022/7/11
 */
public class DynamicConfig extends AbstractConfigBean {

    private static final String CONCURRENCY = "scheme.clone.task.concurrency.%s";

    private static final String DATASOURCE_SOCKET_TIMEOUT = "datasource.socket.timeout";

    private DynamicConfig() {}

    private static class TaskConfigHolder {
        public static final DynamicConfig INSTANCE = new DynamicConfig();
    }

    public static DynamicConfig getInstance() {
        return TaskConfigHolder.INSTANCE;
    }

    public int getConcurrency(String key) {
        return getIntProperty(String.format(CONCURRENCY, key), MAX_ACTIVE);
    }

    public int getDatasourceSocketTimeout() {
        return getIntProperty(DATASOURCE_SOCKET_TIMEOUT, SOCKET_TIMEOUT);
    }
}
