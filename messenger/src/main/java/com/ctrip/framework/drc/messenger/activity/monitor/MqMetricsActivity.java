package com.ctrip.framework.drc.messenger.activity.monitor;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.fetcher.system.TaskQueueActivity;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jixinwang on 2022/10/25
 */
public class MqMetricsActivity extends TaskQueueActivity<MqMonitorContext, Boolean> {

    private static final String measurement = "fx.drc.messenger.produce";
    private static final String measurementMessengerActive = "fx.drc.messenger.active";

    private Map<String, String> tags = Maps.newHashMap();
    private Map<String, String> tagsMessengerActive = Maps.newHashMap();

    @InstanceConfig(path = "cluster")
    public String cluster = "unset";

    // replicator.mhaName: DRC_MQ, so use target.mhaName instead
    @InstanceConfig(path = "target.mhaName")
    public String srcMhaName = "unset";

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        tags.put("clusterName", cluster);
        tags.put("srcMhaName", srcMhaName);
        tagsMessengerActive.put("clusterName", cluster);
        tagsMessengerActive.put("srcMhaName", srcMhaName);
    }

    @Override
    public MqMonitorContext doTask(MqMonitorContext context) {
        if (context.getMetricName() != null && context.getMetricName().contains("messenger.active")) {
            tagsMessengerActive.put("registryKey", context.getRegistryKey());
            tagsMessengerActive.put("sendType", context.getSendMethod());
            DefaultReporterHolder.getInstance().reportMessengerDelay(tagsMessengerActive, (long)context.getValue(), measurementMessengerActive);
        } else {
            tags.put("db", context.getDbName());
            tags.put("table", context.getTableName());
            tags.put("type", context.getEventType().name());
            tags.put("dcTag", context.getDcTag().getName());
            tags.put("topic", context.getTopic());
            tags.put("mqType", context.getMqType());
            tags.put("send", Boolean.toString(context.isSend()));
            DefaultReporterHolder.getInstance().reportResetCounter(tags, (long) context.getValue(), measurement);
        }
        return finish(context);
    }

    @Override
    public int queueSize() {
        return 10000;
    }

    public void report(MqMonitorContext mqMonitorContext) {
        trySubmit(mqMonitorContext);
    }

    @Override
    public void doStop() {
        super.doStop();
        Map<String, String> matchTags = getMatchTags();
        DefaultReporterHolder.getInstance().removeRegister(measurement, matchTags);
        DefaultReporterHolder.getInstance().removeRegister(measurementMessengerActive, matchTags);
    }

    private Map<String, String> getMatchTags() {
        Map<String, String> kvs = new HashMap<>();
        kvs.put("srcMhaName", srcMhaName);
        kvs.put("clusterName", cluster);
        return kvs;
    }
}
