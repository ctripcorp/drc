package com.ctrip.framework.drc.applier.server;

import com.ctrip.framework.drc.applier.activity.event.ApplierDumpEventActivity;
import com.ctrip.framework.drc.applier.activity.event.ApplyActivity;
import com.ctrip.framework.drc.applier.activity.event.TransactionTableApplierDumpEventActivity;
import com.ctrip.framework.drc.applier.activity.monitor.MetricsActivity;
import com.ctrip.framework.drc.applier.activity.monitor.ReportConflictActivity;
import com.ctrip.framework.drc.applier.resource.mysql.DataSourceResource;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.fetcher.activity.event.*;
import com.ctrip.framework.drc.fetcher.resource.condition.CapacityResource;
import com.ctrip.framework.drc.fetcher.resource.condition.LWMResource;
import com.ctrip.framework.drc.fetcher.resource.condition.ListenableDirectMemoryResource;
import com.ctrip.framework.drc.fetcher.resource.condition.ProgressResource;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContextResource;
import com.ctrip.framework.drc.fetcher.resource.thread.ExecutorResource;
import com.ctrip.framework.drc.fetcher.resource.transformer.TransformerContextResource;
import com.ctrip.framework.drc.fetcher.server.FetcherServer;

/**
 * @Author Slight
 * Dec 01, 2019
 */
public class ApplierServerInCluster extends FetcherServer {

    public static final int TRANSACTION_TABLE_APPLY_COUNT = 100;

    public ApplierServerInCluster(ApplierConfigDto config) throws Exception {
        super(config);
    }

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

    public DataSourceResource getDataSourceResource() {
        return ((DataSourceResource) resources.get("DataSource"));
    }

    @Override
    protected int getApplyApplyConcurrency() {
        int applyConcurrency;
        switch (ApplyMode.getApplyMode(config.getApplyMode())) {
            case transaction_table:
                applyConcurrency = TRANSACTION_TABLE_APPLY_COUNT;
                break;
            default:
                applyConcurrency = DEFAULT_APPLY_COUNT;
        }
        return applyConcurrency;
    }

    @Override
    public void doDispose() throws Exception {
        super.doDispose();
    }

    @Override
    public FetcherDumpEventActivity getDumpEventActivity() {
       return ((TransactionTableApplierDumpEventActivity) activities.get("TransactionTableApplierDumpEventActivity"));
    }
}
