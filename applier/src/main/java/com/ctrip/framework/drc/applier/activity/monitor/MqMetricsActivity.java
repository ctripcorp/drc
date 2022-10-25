package com.ctrip.framework.drc.applier.activity.monitor;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.fetcher.system.TaskQueueActivity;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Created by jixinwang on 2022/10/25
 */
public class MqMetricsActivity extends TaskQueueActivity<MqMonitorContext, Boolean> {

    private static final String measurement = "fx.drc.messenger.produce";

    private Map<String, String> tags = Maps.newHashMap();

    @InstanceConfig(path = "cluster")
    public String cluster = "unset";

    @InstanceConfig(path = "replicator.mhaName")
    public String srcMhaName = "unset";

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        tags.put("clusterName", cluster);
        tags.put("srcMhaName", srcMhaName);
    }

    @Override
    public MqMonitorContext doTask(MqMonitorContext context) {
        tags.put("db", context.getDbName());
        tags.put("table", context.getTableName());
        tags.put("type", context.getEventType().getValue());
        tags.put("dcTag", context.getDcTag().getName());

        DefaultReporterHolder.getInstance().reportResetCounter(tags, (long)context.getValue(), measurement);
        return finish(context);
    }

    @Override
    public int queueSize() {
        return 10000;
    }

    public void report(MqMonitorContext mqMonitorContext) {
        trySubmit(mqMonitorContext);
    }

}
