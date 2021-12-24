package com.ctrip.framework.drc.validation.activity.monitor;

import com.ctrip.framework.drc.core.monitor.entity.BaseEntity;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.fetcher.system.TaskQueueActivity;

public class ValidationMetricsActivity extends TaskQueueActivity<ValidationMetricsActivity.ReportUnit, Boolean> {

    private BaseEntity baseEntity;

    @InstanceConfig(path = "appid")
    public long appid = 0;

    @InstanceConfig(path = "bu")
    public String bu = "unset";

    @InstanceConfig(path = "replicator.idc")
    public String dc = "unset";

    @InstanceConfig(path = "cluster")
    public String cluster = "unset";

    @InstanceConfig(path = "mhaName")
    public String mhaName = "unset";

    @InstanceConfig(path = "registryKey")
    public String registryKey = "unset";

    private String formatString;

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        baseEntity = new BaseEntity(appid, bu, dc, cluster, mhaName, registryKey);
        formatString = String.format("%s: %d-%s %s", bu, appid, cluster, mhaName);
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
    public ReportUnit doTask(ReportUnit reportUnit) {
        String measurement = "fx.drc.validation." + reportUnit.name;
        DefaultReporterHolder.getInstance().reportResetCounter(baseEntity.getTags(), reportUnit.value, measurement);
        return finish(reportUnit);
    }

    public void report(String name, long value) {
        trySubmit(new ReportUnit(name, value));
    }

    public static class ReportUnit {

        public String name;
        public Long value;

        public ReportUnit(String name, Long value) {
            this.name = name;
            this.value = value;
        }
    }
}
