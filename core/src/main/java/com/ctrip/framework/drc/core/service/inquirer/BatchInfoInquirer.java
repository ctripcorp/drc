package com.ctrip.framework.drc.core.service.inquirer;

import com.ctrip.framework.drc.core.entity.Instance;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierInfoDto;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorInfoDto;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_MQ;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.QUERY_INFO_LOGGER;

/**
 * @author yongnian
 * @create 2024/12/17 13:42
 */
public class BatchInfoInquirer {

    private static class InnerHolder {
        private static final BatchInfoInquirer INSTANCE = new BatchInfoInquirer();
    }

    public static BatchInfoInquirer getInstance() {
        return InnerHolder.INSTANCE;
    }

    public Pair<List<String>, List<ApplierInfoDto>> getMessengerInfo(List<? extends Instance> messengers) {
        Pair<List<String>, List<ApplierInfoDto>> applierInfoInner = getApplierInfoInner(messengers);
        applierInfoInner.getValue().removeIf(e -> !e.getRegistryKey().contains(DRC_MQ));
        return applierInfoInner;
    }

    public Pair<List<String>, List<ApplierInfoDto>> getApplierInfo(List<? extends Instance> appliers) {
        Pair<List<String>, List<ApplierInfoDto>> applierInfoInner = getApplierInfoInner(appliers);
        applierInfoInner.getValue().removeIf(e -> e.getRegistryKey().contains(DRC_MQ));
        return applierInfoInner;
    }

    public Pair<List<String>, List<ApplierInfoDto>> getApplierInfoInner(List<? extends Instance> appliers) {
        List<Pair<String, Integer>> applierIpAndPorts = appliers.stream().map(applier -> Pair.from(applier.getIp(), applier.getPort())).distinct().collect(Collectors.toList());
        ApplierInfoInquirer infoInquirer = ApplierInfoInquirer.getInstance();
        List<Future<List<ApplierInfoDto>>> futures = Lists.newArrayList();
        for (Pair<String, Integer> pair : applierIpAndPorts) {
            futures.add(infoInquirer.query(pair.getKey() + ":" + pair.getValue()));
        }
        List<ApplierInfoDto> applierInfoDtos = Lists.newArrayList();
        List<String> validIps = Lists.newArrayList();
        for (int i = 0; i < futures.size(); i++) {
            Future<List<ApplierInfoDto>> future = futures.get(i);
            Pair<String, Integer> pair = applierIpAndPorts.get(i);
            String ip = pair.getKey();
            Integer port = pair.getValue();
            try {
                List<ApplierInfoDto> infoDtos = future.get(1000, TimeUnit.MILLISECONDS);
                infoDtos.forEach(e -> {
                    e.setIp(ip);
                    e.setPort(port);
                });
                applierInfoDtos.addAll(infoDtos);
                validIps.add(ip);
                EventMonitor.DEFAULT.logEvent("drc.inquiry.applier.success", ip + ":" + port);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                future.cancel(true);
                QUERY_INFO_LOGGER.error("get applier fail, skip for: {}", pair.getKey() + ":" + pair.getValue());
                EventMonitor.DEFAULT.logEvent("drc.inquiry.applier.fail", ip + ":" + port);
            }
        }
        return Pair.from(validIps, applierInfoDtos);
    }

    public Pair<List<String>, List<ReplicatorInfoDto>> getReplicatorInfo(List<? extends Instance> replicators) {
        List<Pair<String, Integer>> applierIpAndPorts = replicators.stream().map(applier -> Pair.from(applier.getIp(), applier.getPort())).distinct().collect(Collectors.toList());
        ReplicatorInfoInquirer infoInquirer = ReplicatorInfoInquirer.getInstance();
        List<Future<List<ReplicatorInfoDto>>> futures = Lists.newArrayList();
        for (Pair<String, Integer> pair : applierIpAndPorts) {
            futures.add(infoInquirer.query(pair.getKey() + ":" + pair.getValue()));
        }
        List<ReplicatorInfoDto> replicatorInfoDtos = Lists.newArrayList();
        List<String> validIps = Lists.newArrayList();
        for (int i = 0; i < futures.size(); i++) {
            Future<List<ReplicatorInfoDto>> future = futures.get(i);
            Pair<String, Integer> pair = applierIpAndPorts.get(i);
            String ip = pair.getKey();
            Integer port = pair.getValue();
            try {
                List<ReplicatorInfoDto> infoDtos = future.get(1000, TimeUnit.MILLISECONDS);
                infoDtos.forEach(e -> {
                    e.setIp(ip);
                    e.setPort(port);
                });
                replicatorInfoDtos.addAll(infoDtos);
                validIps.add(ip);
                EventMonitor.DEFAULT.logEvent("drc.inquiry.replicator.success", ip + ":" + port);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                QUERY_INFO_LOGGER.error("get replicator fail, skip for: {}", pair.getKey() + ":" + pair.getValue());
                EventMonitor.DEFAULT.logEvent("drc.inquiry.replicator.fail", ip + ":" + port);
            }
        }
        return Pair.from(validIps, replicatorInfoDtos);
    }
}
