package com.ctrip.framework.drc.applier.server;

import com.ctrip.framework.drc.applier.activity.event.ApplierDumpEventActivity;
import com.ctrip.framework.drc.applier.activity.event.ApplyActivity;
import com.ctrip.framework.drc.applier.activity.event.TransactionTableApplierDumpEventActivity;
import com.ctrip.framework.drc.applier.activity.event.TransactionTableApplyActivity;
import com.ctrip.framework.drc.applier.activity.monitor.MetricsActivity;
import com.ctrip.framework.drc.applier.activity.monitor.ReportConflictActivity;
import com.ctrip.framework.drc.applier.resource.mysql.DataSourceResource;
import com.ctrip.framework.drc.core.config.TestConfig;
import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.fetcher.activity.event.*;
import com.ctrip.framework.drc.fetcher.resource.condition.CapacityResource;
import com.ctrip.framework.drc.fetcher.resource.condition.LWMResource;
import com.ctrip.framework.drc.fetcher.resource.condition.ListenableDirectMemoryResource;
import com.ctrip.framework.drc.fetcher.resource.condition.ProgressResource;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContextResource;
import com.ctrip.framework.drc.fetcher.resource.position.TransactionTableResource;
import com.ctrip.framework.drc.fetcher.resource.thread.ExecutorResource;
import com.ctrip.framework.drc.fetcher.resource.transformer.TransformerContextResource;
import com.ctrip.framework.drc.fetcher.server.FetcherServer;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;

import static com.ctrip.framework.drc.applier.server.ApplierServerInCluster.TRANSACTION_TABLE_APPLY_COUNT;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.COMMA;

/**
 * @Author Slight
 * Sep 18, 2019
 */
public class LocalApplierServer extends FetcherServer {

    private ApplyMode applyMode;

    public LocalApplierServer() throws Exception {
        this(3306, 8383, SystemConfig.INTEGRITY_TEST, Sets.newHashSet(), new TestConfig(ApplyMode.set_gtid, null, null));
    }

    public LocalApplierServer(int destMySQLPort, int replicatorPort, String destination, Set<String> includedDb, TestConfig dstConfig) throws Exception {
        this.applyMode = dstConfig.getApplyMode();
        ApplierConfigDto config = createApplierDto(destMySQLPort, replicatorPort, destination, includedDb, dstConfig);
        setConfig(config, ApplierConfigDto.class);
        setName(config.getRegistryKey());
        define();
    }

    public ApplierConfigDto createApplierDto(int destMySQLPort, int replicatorPort, String registryKey, Set<String> includedDb, TestConfig dstConfig) {
        ApplierConfigDto config = new ApplierConfigDto();
        config.setGtidExecuted("");
        config.setIdc("dest");
        config.setCluster("cluster");
        config.setName("[" + replicatorPort + "]->LOCAL_APPLIER->[" + destMySQLPort + "]");
        config.setIncludedDbs(StringUtils.join(includedDb, COMMA));

        if (ApplyMode.mq == applyMode) {
            config.setProperties(dstConfig.getProperties());
        } else {
            config.setProperties(dstConfig.getRowsFilter());
        }

        config.setApplyMode(dstConfig.getApplyMode().getType());
        config.setNameFilter(dstConfig.getNameFilter());

        InstanceInfo replicator = new InstanceInfo();
        replicator.setIdc("src");
        replicator.setIp("127.0.0.1");
        replicator.setPort(replicatorPort);
        replicator.setMhaName("mha2");

        DBInfo db = new DBInfo();
        db.setIp("127.0.0.1");
        db.setPort(destMySQLPort);
        db.setUsername("root");
        db.setPassword("root");
        db.setMhaName("mha1");

        config.setReplicator(replicator);
        config.setTarget(db);
        return config;
    }

    @Override
    public void define() throws Exception {
        if (ApplyMode.set_gtid == applyMode) {
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
            return;
        }

        if (ApplyMode.transaction_table == applyMode) {
            defineTransactionTable();
            return;
        }

//        if (ApplyMode.mq == applyMode) {
//            defineMessenger();
//        }
    }

    public void defineTransactionTable() throws Exception {
        source(TransactionTableApplierDumpEventActivity.class)
                .with(ExecutorResource.class)
                .with(LinkContextResource.class)
                .with(DataSourceResource.class)
                .with(LWMResource.class)
                .with(ProgressResource.class)
                .with(CapacityResource.class)
                .with(ListenableDirectMemoryResource.class)
                .with(TransactionTableResource.class)
                .with(TransformerContextResource.class)
                .with(MetricsActivity.class)
                .with(ReportConflictActivity.class)
                .with(LoadEventActivity.class)
                .link(InvolveActivity.class)
                .link(ApplierGroupActivity.class)
                .link(DispatchActivity.class)
                .link(TransactionTableApplyActivity.class, 100)
                .link(CommitActivity.class);
        check();
    }

//    public void defineMessenger() throws Exception {
//        source(MqApplierDumpEventActivity.class)
//                .with(ExecutorResource.class)
//                .with(LinkContextResource.class)
//                .with(LWMResource.class)
//                .with(ProgressResource.class)
//                .with(CapacityResource.class)
//                .with(ListenableDirectMemoryResource.class)
//                .with(MqProviderResource.class)
//                .with(MqPositionResource.class)
//                .with(MqMetricsActivity.class)
//                .with(LoadEventActivity.class)
//                .link(InvolveActivity.class)
//                .link(ApplierGroupActivity.class)
//                .link(DispatchActivity.class)
//                .link(MqApplyActivity.class, 1)
//                .link(CommitActivity.class);
//        check();
//    }

    @Override
    public FetcherDumpEventActivity getDumpEventActivity() {
        return ((TransactionTableApplierDumpEventActivity) activities.get("TransactionTableApplierDumpEventActivity"));
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
}
