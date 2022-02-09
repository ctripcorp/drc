package com.ctrip.framework.drc.applier.server;

import com.ctrip.framework.drc.applier.activity.event.*;
import com.ctrip.framework.drc.applier.activity.monitor.MetricsActivity;
import com.ctrip.framework.drc.applier.activity.monitor.ReportConflictActivity;
import com.ctrip.framework.drc.applier.resource.condition.LWMResource;
import com.ctrip.framework.drc.applier.resource.condition.ProgressResource;
import com.ctrip.framework.drc.applier.resource.mysql.DataSourceResource;
import com.ctrip.framework.drc.fetcher.activity.event.InvolveActivity;
import com.ctrip.framework.drc.fetcher.activity.event.LoadEventActivity;
import com.ctrip.framework.drc.fetcher.resource.condition.CapacityResource;
import com.ctrip.framework.drc.fetcher.resource.condition.ListenableDirectMemoryResource;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContextResource;
import com.ctrip.framework.drc.fetcher.resource.thread.ExecutorResource;
import com.ctrip.framework.drc.fetcher.resource.transformer.TransformerContextResource;
import com.ctrip.framework.drc.fetcher.system.AbstractLink;

/**
 * @Author Slight
 * Oct 24, 2019
 */
public class ApplierServer extends AbstractLink {

    public void define() throws Exception {
            source(ApplierDumpEventActivity.class)
                    .with(ExecutorResource.class)
                    .with(LinkContextResource.class)
                    .with(DataSourceResource.class)
                    .with(LWMResource.class)
                    .with(ProgressResource.class)
                    .with(CapacityResource.class)
                    .with(ListenableDirectMemoryResource.class)
                    .with(TransformerContextResource.class)
                    .with(MetricsActivity.class)
                    .with(ReportConflictActivity.class)
                    .with(LoadEventActivity.class)
                    .link(InvolveActivity.class)
                    .link(ApplierGroupActivity.class)
                    .link(DispatchActivity.class)
                    .link(ApplyActivity.class, 100)
                    .link(CommitActivity.class);
            check();
    }

    public long getLWM() {
        return ((LWMResource) resources.get("LWM")).current();
    }

    public long getProgress() {
        return ((ProgressResource) resources.get("Progress")).get();
    }

    public DataSourceResource getDataSourceResource() {
        return ((DataSourceResource) resources.get("DataSource"));
    }

}
