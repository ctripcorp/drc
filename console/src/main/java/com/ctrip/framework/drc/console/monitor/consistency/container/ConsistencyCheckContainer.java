package com.ctrip.framework.drc.console.monitor.consistency.container;

import com.ctrip.framework.drc.console.monitor.consistency.instance.ConsistencyCheck;
import com.ctrip.framework.drc.console.monitor.consistency.instance.DefaultConsistencyCheck;
import com.ctrip.framework.drc.console.monitor.consistency.instance.FullDataConsistencyCheck;
import com.ctrip.framework.drc.console.monitor.consistency.instance.InstanceConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.FullDataConsistencyMonitorConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.core.monitor.entity.ConsistencyEntity;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.console.monitor.consistency.sql.execution.TimeRangeQuerySingleExecution.TIME_INTERVAL_SECOND;

/**
 * Created by mingdongli
 * 2019/11/19 下午4:24.
 */
public class ConsistencyCheckContainer extends AbstractLifecycle {

    private Logger logger = LoggerFactory.getLogger("consistencyMonitorLogger");

    private Map<String, ConsistencyCheck> consistencyCheckMap = Maps.newConcurrentMap();

    private ScheduledExecutorService checkScheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("ConsistencyCheckContainer-Check");

    private ExecutorService workExecutorService = ThreadUtils.newCachedThreadPool("ConsistencyCheckContainer-Work");

    private Reporter reporter = DefaultReporterHolder.getInstance();

    public Set<String> getConsistencyCheckSet() {
        return Sets.newHashSet(consistencyCheckMap.keySet());
    }

    public boolean addConsistencyCheck(InstanceConfig instanceConfig) throws Exception {
        ConsistencyCheck consistencyCheck = new DefaultConsistencyCheck(instanceConfig);
        consistencyCheckMap.put(instanceConfig.getDelayMonitorConfig().getTableSchema(), consistencyCheck);
        consistencyCheck.initialize();
        consistencyCheck.start();
        DefaultEventMonitorHolder.getInstance().logEvent("Console.Consistency.Add", instanceConfig.getDelayMonitorConfig().getTableSchema());
        return true;
    }

    public void removeConsistencyCheck(String tableSchema) throws Exception {
        if (consistencyCheckMap.containsKey(tableSchema)) {
            ConsistencyCheck consistencyCheck = consistencyCheckMap.get(tableSchema);
            consistencyCheck.stop();
            consistencyCheck.dispose();
            ConsistencyCheck removedCheck = consistencyCheckMap.remove(tableSchema);
            boolean flag = false;
            if (null != removedCheck) {
                DefaultConsistencyCheck check = (DefaultConsistencyCheck) removedCheck;
                ConsistencyEntity consistencyEntity = check.getConsistencyEntity();
                flag = reporter.removeRegister(consistencyEntity.getTags(), "fx.drc.dataCheck.compareNum");
            }
            if (flag) DefaultEventMonitorHolder.getInstance().logEvent("Console.Consistency.Delete", tableSchema);
            else
                logger.info("[Console.Consistency.Delete] delete fail,tableSchema no exist or no register: {}", tableSchema);
        }
    }

    @Override
    protected void doStart() {
        checkScheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    if ("on".equalsIgnoreCase(MonitorTableSourceProvider.getInstance().getDataConsistentMonitorSwitch())) {
                        doCheck();
                    }
                } catch (Throwable t) {
                    logger.error("date consistency check schedule error", t);
                }
            }
        }, 10, TIME_INTERVAL_SECOND, TimeUnit.SECONDS);
    }

    private void doCheck() {
        Map<String, ConsistencyCheck> copyCheckMap = Maps.newHashMap(consistencyCheckMap);
        int res = 0;
        for (Map.Entry<String, ConsistencyCheck> entry : copyCheckMap.entrySet()) {
            workExecutorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.console.consistency", entry.getKey(), new Task() {
                            @Override
                            public void go() {
                                entry.getValue().check();
                            }
                        });
                    } catch (Exception e) {
                        logger.error("consistency check error", e);
                    }
                }
            });
            res++;
            logger.info("[workExecutorService],checkWorkerRes is : {}", res);
        }
    }

    public Set<String> addAndFullDataConsistencyCheck(FullDataConsistencyMonitorConfig fullDataConsistencyMonitorConfig, Endpoint mhaAEndpoint, Endpoint mhaBEndpoint) throws Exception {
        ConsistencyCheck consistencyCheck = new FullDataConsistencyCheck(fullDataConsistencyMonitorConfig, mhaAEndpoint, mhaBEndpoint);
        consistencyCheck.initialize();
        consistencyCheck.start();
        return consistencyCheck.getDiff();
    }
}
