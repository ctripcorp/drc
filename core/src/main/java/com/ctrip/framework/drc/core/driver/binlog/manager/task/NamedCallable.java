package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import org.slf4j.Logger;

import java.util.concurrent.Callable;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DDL_LOGGER;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public interface NamedCallable<V> extends Callable<V> {

    default String name() {
        return getClass().getSimpleName();
    }

    default void afterException(Throwable t) {
        getLogger().error("exec {} error", name(), t);
    }

    default void afterFail() {
        DefaultEventMonitorHolder.getInstance().logEvent("RetryTask", name());
    }

    default void afterSuccess(int retryTime) {
        getLogger().info("{} success with retryTime {}", name(), retryTime);
    }

    default Logger getLogger() {
        return DDL_LOGGER;
    }
}
