package com.ctrip.framework.drc.console.monitor.consistency.instance;

import com.ctrip.framework.drc.console.monitor.consistency.sql.operator.FullDataStreamSqlOperatorWrapper;
import com.ctrip.framework.drc.console.monitor.consistency.table.DefaultTableNameDescriptor;
import com.ctrip.framework.drc.console.monitor.consistency.table.TableNameDescriptor;
import com.ctrip.framework.drc.console.monitor.consistency.task.FullDataRangeQueryTask;
import com.ctrip.framework.drc.console.monitor.delay.config.FullDataConsistencyMonitorConfig;
import com.ctrip.framework.drc.core.service.utils.Constants;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by jixinwang on 2021/2/20
 */
public class FullDataConsistencyCheck extends AbstractLifecycle implements ConsistencyCheck {
    protected Logger logger = LoggerFactory.getLogger("consistencyMonitorLogger");

    private TableNameDescriptor tableDescriptor;

    private FullDataStreamSqlOperatorWrapper src;

    private FullDataStreamSqlOperatorWrapper dst;

    private FullDataRangeQueryTask fullDataRangeQueryTask;

    private Set<String> keys;

    public FullDataConsistencyCheck(FullDataConsistencyMonitorConfig fullDataConsistencyMonitorConfig, Endpoint mhaAEndpoint, Endpoint mhaBEndpoint) {
        tableDescriptor = new DefaultTableNameDescriptor(fullDataConsistencyMonitorConfig.getTableSchema(), fullDataConsistencyMonitorConfig.getKey(), fullDataConsistencyMonitorConfig.getOnUpdate());
        fullDataRangeQueryTask = new FullDataRangeQueryTask(tableDescriptor, fullDataConsistencyMonitorConfig.getStartTimestamp(), fullDataConsistencyMonitorConfig.getEndTimeStamp());

        src = new FullDataStreamSqlOperatorWrapper(mhaAEndpoint);
        dst = new FullDataStreamSqlOperatorWrapper(mhaBEndpoint);
    }

    public Set<String> getKeys() {
        return keys;
    }

    public void setKeys(Set<String> keys) {
        this.keys = keys;
    }

    @Override
    protected void doInitialize() throws Exception {
        src.initialize();
        dst.initialize();
    }

    @Override
    protected void doStart() throws Exception {
        check();
    }

    @Override
    public boolean check() {
        Set<String> keys = fullDataRangeQueryTask.calculate(src, dst).keySet();
        if (keys.isEmpty()) {
            logger.info("[FULL DATA CHECK] success table: {}, key: {}", tableDescriptor.getTable(), tableDescriptor.getKey());
        } else {
            if (keys.size() > Constants.hundred) {
                keys = keys.stream().limit(Constants.hundred).collect(Collectors.toSet());
            }
            logger.error("[FULL DATA CHECK] error table: {}, key: {}, errorKeys is {}", tableDescriptor.getTable(), tableDescriptor.getKey(), StringUtils.join(keys, ","));
        }
        setKeys(keys);
        return true;
    }

    @Override
    public Set<String> getDiff() {
        return getKeys();
    }
}
