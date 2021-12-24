package com.ctrip.framework.drc.console.monitor;

import java.util.concurrent.TimeUnit;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-07-27
 */
public interface Monitor {

    void initialize();

    void start();

    void scheduledTask();

    void destroy();

    int getDefaultInitialDelay();

    int getDefaultPeriod();

    TimeUnit getDefaultTimeUnit();

}
