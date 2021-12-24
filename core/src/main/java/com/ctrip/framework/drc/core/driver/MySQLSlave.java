package com.ctrip.framework.drc.core.driver;

import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;

/**
 * input: ip, port, user, password
 * output: LogEvent
 *
 * @author wenchao.meng
 * <p>
 * Sep 01, 2019
 */
public interface MySQLSlave extends Lifecycle {

    void setLogEventHandler(LogEventHandler logEventHandler);

}
