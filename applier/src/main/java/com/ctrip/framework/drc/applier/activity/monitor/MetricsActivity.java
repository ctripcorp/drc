package com.ctrip.framework.drc.applier.activity.monitor;

import com.ctrip.framework.drc.core.monitor.entity.UnidirectionalEntity;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.fetcher.system.DelayReporter;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.fetcher.system.TaskQueueActivity;
import com.google.common.collect.Maps;

import java.util.Map;

public class MetricsActivity extends TaskQueueActivity<MetricsActivity.Delay, Boolean> implements DelayReporter {

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
    public Delay doTask(Delay delay) {
        String measurement = "fx.drc.applier." + delay.name;
        if(measurement.contains("conflict")) {
            DefaultReporterHolder.getInstance().reportResetCounter(unidirectionalEntity.getTags(), delay.value, measurement);
        } else if (measurement.contains("transaction")) {
            Map<String, String> tags = unidirectionalEntity.getTags();
            tags.put("db", delay.dbName);
            DefaultReporterHolder.getInstance().reportResetCounter(tags, delay.value, measurement);
            tags.remove("db");
            DefaultReporterHolder.getInstance().reportResetCounter(instanceTags, delay.value, measurement);
        } else {
            DefaultReporterHolder.getInstance().reportDelay(unidirectionalEntity, delay.value, measurement);
        }
        return finish(delay);
    }

    @Override
    public void report(String name, String dbName, long value) {
        trySubmit(new Delay(name, dbName, value));
    }

    public static class Delay {

        public String name;
        public String dbName;
        public Long value;

        public Delay(String name, String dbName, Long value) {
            this.name = name;
            this.dbName = dbName;
            this.value = value;
        }
    }
}
