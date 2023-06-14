package com.ctrip.framework.drc.applier.activity.monitor;

import com.ctrip.framework.drc.core.monitor.entity.UnidirectionalEntity;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.fetcher.system.MetricReporter;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.fetcher.system.TaskQueueActivity;
import com.google.common.collect.Maps;

import java.util.Map;

public class MetricsActivity extends TaskQueueActivity<MetricsActivity.Metric, Boolean> implements MetricReporter {

    private UnidirectionalEntity unidirectionalEntity;

    private Map<String, String> instanceTags;

    @InstanceConfig(path = "appid")
    public long appid = 0;

    @InstanceConfig(path = "cluster")
    public String cluster = "unset";

    @InstanceConfig(path = "bu")
    public String bu = "unset";

    @InstanceConfig(path = "idc")
    public String dest = "unset";

    @InstanceConfig(path = "replicator.idc")
    public String src = "unset";

    @InstanceConfig(path = "replicator.mhaName")
    public String srcMhaName = "unset";

    @InstanceConfig(path = "target.mhaName")
    public String destMhaName = "unset";

    private String formatString;

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        unidirectionalEntity = new UnidirectionalEntity.Builder()
                .clusterAppId(appid)
                .buName(bu)
                .srcDcName(src)
                .destDcName(dest)
                .clusterName(cluster)
                .srcMhaName(srcMhaName)
                .destMhaName(destMhaName)
                .build();
        instanceTags = Maps.newHashMap();
        instanceTags.put("db", "allDbsInApplier");
        formatString = String.format("%s: %d-%s %s->%s", bu, appid, cluster, src, dest);
    }

    @Override
    public String toString() {
        return formatString;
    }

    @Override
    public int queueSize() {
        return 10000;
    }

    @Override
    public Metric doTask(Metric metric) {
        String measurement = "fx.drc.applier." + metric.name;
        if(measurement.contains("conflict")) {
            Map<String,String> tags = metric.tags;
            tags.putAll(unidirectionalEntity.getTags());
            DefaultReporterHolder.getInstance().reportResetCounter(tags, metric.value, measurement);
        } else if (measurement.contains("transaction") || measurement.contains("rows")) {
            Map<String, String> tags = unidirectionalEntity.getTags();
            tags.put("db", metric.tags.get("dbName"));
            DefaultReporterHolder.getInstance().reportResetCounter(tags, metric.value, measurement);
            tags.remove("db");
            DefaultReporterHolder.getInstance().reportResetCounter(instanceTags, metric.value, measurement);
        } else {
            DefaultReporterHolder.getInstance().reportDelay(unidirectionalEntity, metric.value, measurement);
        }
        return finish(metric);
    }

   

    @Override
    public void report(String name, Map<String,String> tags, long value) {
        trySubmit(new Metric(name,  tags, value));
    }


    public static class Metric {

        public String name;
        public Map<String,String> tags;
        public Long value;
        

        public Metric(String name, Map<String,String> tags, Long value) {
            this.name = name;
            this.tags = tags;
            this.value = value;
        }
    }
}
