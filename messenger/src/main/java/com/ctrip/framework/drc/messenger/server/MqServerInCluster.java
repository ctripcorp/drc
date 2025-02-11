package com.ctrip.framework.drc.messenger.server;

import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerConfigDto;
import com.ctrip.framework.drc.fetcher.activity.event.*;
import com.ctrip.framework.drc.fetcher.resource.condition.CapacityResource;
import com.ctrip.framework.drc.fetcher.resource.condition.LWMResource;
import com.ctrip.framework.drc.fetcher.resource.condition.ListenableDirectMemoryResource;
import com.ctrip.framework.drc.fetcher.resource.condition.ProgressResource;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContextResource;
import com.ctrip.framework.drc.fetcher.resource.context.MqPosition;
import com.ctrip.framework.drc.fetcher.resource.thread.ExecutorResource;
import com.ctrip.framework.drc.fetcher.server.FetcherServer;
import com.ctrip.framework.drc.fetcher.system.qconfig.FetcherDynamicConfig;
import com.ctrip.framework.drc.messenger.activity.event.MqApplierDumpEventActivity;
import com.ctrip.framework.drc.messenger.activity.event.MqApplyActivity;
import com.ctrip.framework.drc.messenger.activity.monitor.MqMetricsActivity;
import com.ctrip.framework.drc.messenger.mq.MqPositionResource;
import com.ctrip.framework.drc.messenger.mq.MqProviderResource;
import com.ctrip.framework.drc.messenger.resource.thread.MqRowEventExecutorResource;

/**
 * Created by jixinwang on 2022/10/12
 */
public class MqServerInCluster extends FetcherServer {

    public MqServerInCluster(MessengerConfigDto config) throws Exception {
        super(config);
    }

    @Override
    public void define() throws Exception {
        logger.info("mq apply concurrency : {} for: {}", config.getApplyConcurrency(), config.getRegistryKey());
        source(MqApplierDumpEventActivity.class)
                .with(ExecutorResource.class)
                .with(MqRowEventExecutorResource.class)
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
                .link(MqApplyActivity.class, config.getApplyConcurrency())
                .link(CommitActivity.class);
        check();
    }

    @Override
    protected int getApplyApplyConcurrency() {
        String registryKey = config.getRegistryKey();
        int applyConcurrency;
        switch (ApplyMode.getApplyMode(config.getApplyMode())) {
            case mq:
                applyConcurrency = FetcherDynamicConfig.getInstance().getMqApplyCount(registryKey);
                break;
            default:
                applyConcurrency = DEFAULT_APPLY_COUNT;
        }
        return applyConcurrency;
    }

    @Override
    public FetcherDumpEventActivity getDumpEventActivity() {
        return ((MqApplierDumpEventActivity) activities.get("MqApplierDumpEventActivity"));
    }

    public MqPosition getMqPositionResource() {
        return ((MqPosition) resources.get("MqPosition"));
    }

}
