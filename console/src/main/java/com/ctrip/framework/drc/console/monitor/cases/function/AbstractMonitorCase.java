package com.ctrip.framework.drc.console.monitor.cases.function;

import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-07
 */
public abstract class AbstractMonitorCase implements MonitorCase {

    Logger logger = LoggerFactory.getLogger(getClass());

    Map<Endpoint, WriteSqlOperatorWrapper> sqlOperatorMapper = new ConcurrentHashMap<>();

    protected WriteSqlOperatorWrapper getSqlOperatorWrapper(Endpoint endpoint) {
        if(sqlOperatorMapper.containsKey(endpoint)) {
            return sqlOperatorMapper.get(endpoint);
        } else {
            WriteSqlOperatorWrapper sqlOperatorWrapper = new WriteSqlOperatorWrapper(endpoint);
            try {
                sqlOperatorWrapper.initialize();
                sqlOperatorWrapper.start();
                logger.info("[[endpoint={}:{}]]sqlOperatorWrapper initialized and started", endpoint.getHost(), endpoint.getPort());
            } catch (Exception e) {
                logger.error("[[endpoint={}:{}]]sqlOperatorWrapper initialize: ", endpoint.getHost(), endpoint.getPort(), e);
            }
            sqlOperatorMapper.put(endpoint, sqlOperatorWrapper);
            return sqlOperatorWrapper;
        }
    }

    protected void removeSqlOperator(Endpoint endpoint) {
        WriteSqlOperatorWrapper writeSqlOperatorWrapper = sqlOperatorMapper.remove(endpoint);
        if (writeSqlOperatorWrapper != null) {
            try {
                writeSqlOperatorWrapper.stop();
                writeSqlOperatorWrapper.dispose();
            } catch (Exception e) {
                logger.error("[[monitor=delay,endpoint={}:{}]]PeriodicalUpdateDbTask sqlOperatorWrapper stop and dispose: ", endpoint.getHost(), endpoint.getPort(), e);
            }
        }
    }
}
