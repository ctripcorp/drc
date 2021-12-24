package com.ctrip.framework.drc.core.driver.binlog;

import com.ctrip.framework.drc.core.exception.dump.BinlogDumpGtidException;

/**
 * @author wenchao.meng
 * <p>
 * Sep 01, 2019
 */
@FunctionalInterface
public interface LogEventHandler {

    void onLogEvent(LogEvent logEvent, LogEventCallBack logEventCallBack, BinlogDumpGtidException exception);

}
