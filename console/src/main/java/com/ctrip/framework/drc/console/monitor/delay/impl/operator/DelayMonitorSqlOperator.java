package com.ctrip.framework.drc.console.monitor.delay.impl.operator;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.PoolProperties;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-05
 */
public class DelayMonitorSqlOperator extends ReadWriteAbstractSqlOperator {
    public DelayMonitorSqlOperator(Endpoint endpoint, PoolProperties poolProperties) {
        super(endpoint, poolProperties);
    }
}
