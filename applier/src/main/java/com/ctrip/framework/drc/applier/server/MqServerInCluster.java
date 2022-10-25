package com.ctrip.framework.drc.applier.server;

import com.ctrip.framework.drc.applier.activity.event.*;
import com.ctrip.framework.drc.applier.activity.monitor.MqMetricsActivity;
import com.ctrip.framework.drc.applier.mq.MqPositionResource;
import com.ctrip.framework.drc.applier.mq.MqProviderResource;
import com.ctrip.framework.drc.applier.resource.condition.LWMResource;
import com.ctrip.framework.drc.applier.resource.condition.ProgressResource;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.fetcher.activity.event.InvolveActivity;
import com.ctrip.framework.drc.fetcher.activity.event.LoadEventActivity;
import com.ctrip.framework.drc.fetcher.resource.condition.CapacityResource;
import com.ctrip.framework.drc.fetcher.resource.condition.ListenableDirectMemoryResource;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContextResource;
import com.ctrip.framework.drc.fetcher.resource.thread.ExecutorResource;

/**
 * Created by jixinwang on 2022/10/12
 */
public class MqServerInCluster extends ApplierServerInCluster {

    public MqServerInCluster(ApplierConfigDto config) throws Exception {
        super(config);
    }

    @Override
    public void define() throws Exception {
        source(MqApplierDumpEventActivity.class)
                .with(ExecutorResource.class)
                .with(LinkContextResource.class)
                .with(LWMResource.class)
                .with(ProgressResource.class)
                .with(CapacityResource.class)
                .with(ListenableDirectMemoryResource.class)
                .with(MqProviderResource.class)
                .with(MqPositionResource.class)
                .with(MqMetricsActivity.class)
                .with(LoadEventActivity.class)
                .link(InvolveActivity.class)
                .link(ApplierGroupActivity.class)
                .link(DispatchActivity.class)
                .link(MqApplyActivity.class, 1)
                .link(CommitActivity.class);
        check();
    }
}
