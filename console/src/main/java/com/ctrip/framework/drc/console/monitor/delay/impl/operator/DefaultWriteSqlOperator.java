package com.ctrip.framework.drc.console.monitor.delay.impl.operator;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.PoolProperties;

/**
 * Created by dengquanliang
 * 2023/12/28 15:41
 */
public class DefaultWriteSqlOperator extends AbstractWriteSqlOperator {
    public DefaultWriteSqlOperator(Endpoint endpoint, PoolProperties poolProperties) {
        super(endpoint, poolProperties);
    }
}
