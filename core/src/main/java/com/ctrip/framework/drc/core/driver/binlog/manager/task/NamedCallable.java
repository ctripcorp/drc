package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public interface NamedCallable<V> extends Callable<V> {

    Logger DDL_LOGGER = LoggerFactory.getLogger("com.ctrip.framework.drc.replicator.impl.inbound.filter.DdlFilter");

    default String name() {
        return getClass().getSimpleName();
    }

    default void afterException(Throwable t) {
        DDL_LOGGER.error("exec {} error", name(), t);
    }

    default void afterFail() {
        DefaultEventMonitorHolder.getInstance().logEvent("RetryTask", name());
    }

    default void afterSuccess(int retryTime) {
        DDL_LOGGER.info("{} success with retryTime {}", name(), retryTime);
    }
}
