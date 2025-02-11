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
import com.ctrip.framework.drc.messenger.mq.KafkaPositionResource;
import com.ctrip.framework.drc.messenger.mq.MqProviderResource;

/**
 * Created by dengquanliang
 * 2025/1/9 16:10
 */
public class KafkaServerInCluster extends FetcherServer {

    public KafkaServerInCluster(MessengerConfigDto config) throws Exception {
        super(config);
    }

    @Override
    public void define() throws Exception {
        logger.info("mq apply concurrency : {} for: {}", config.getApplyConcurrency(), config.getRegistryKey());
        source(MqApplierDumpEventActivity.class)
                .with(ExecutorResource.class)
                .with(LinkContextResource.class)
                .with(LWMResource.class)
                .with(ProgressResource.class)
                .with(CapacityResource.class)
                .with(ListenableDirectMemoryResource.class)
                .with(MqProviderResource.class)
                .with(KafkaPositionResource.class, "MqPosition")
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
            case kafka:
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
        return ((KafkaPositionResource) resources.get("MqPosition"));
    }
}
