package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.xpipe.config.AbstractConfigBean;

import static com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager.MAX_ACTIVE;

/**
 * Created by jixinwang on 2022/7/11
 */
public class TaskConfig extends AbstractConfigBean {

    private static final String CONCURRENCY = "scheme.clone.task.concurrency.%s";

    private TaskConfig() {}

    private static class TaskConfigHolder {
        public static final TaskConfig INSTANCE = new TaskConfig();
    }

    public static TaskConfig getInstance() {
        return TaskConfigHolder.INSTANCE;
    }

    public Integer getConcurrency(String key) {
        return getIntProperty(String.format(CONCURRENCY, key), MAX_ACTIVE);
    }
}
