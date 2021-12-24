package com.ctrip.framework.drc.core.monitor.operator;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import org.apache.tomcat.jdbc.pool.PoolProperties;

/**
 * Created by mingdongli
 * 2019/10/9 下午4:11.
 */
public interface SqlOperator extends Lifecycle {

    PoolProperties getPoolProperties();
}
