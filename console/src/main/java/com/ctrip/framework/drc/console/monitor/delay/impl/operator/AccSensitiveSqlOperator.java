package com.ctrip.framework.drc.console.monitor.delay.impl.operator;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.AccountEndpoint;
import org.apache.tomcat.jdbc.pool.PoolProperties;

/**
 * @ClassName AccSensitiveSqlOperator
 * @Author haodongPan
 * @Date 2024/6/19 14:18
 * @Version: $
 */
public class AccSensitiveSqlOperator extends ReadWriteAbstractSqlOperator {
    
    public AccSensitiveSqlOperator(AccountEndpoint endpoint, PoolProperties poolProperties) {
        super(endpoint, poolProperties);
    }

    @Override
    protected void doInitialize() throws Exception {
        dataSource = dataSourceManager.getDataSourceForAccountValidate(endpoint, poolProperties);
    }

    @Override
    protected void doStop() throws Exception {
        dataSourceManager.clearDataSourceForAccountValidate(endpoint);
    }
    
    
}
