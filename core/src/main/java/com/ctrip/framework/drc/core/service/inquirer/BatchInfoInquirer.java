package com.ctrip.framework.drc.core.service.inquirer;

import com.ctrip.framework.drc.core.entity.Instance;
import com.ctrip.framework.drc.core.server.config.InfoDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierInfoDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerInfoDto;
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

    public Pair<List<String>, List<MessengerInfoDto>> getMessengerInfo(List<? extends Instance> messengers) {
        return getInstanceInfo(MessengerInfoInquirer.getInstance(), messengers);

    }

    public Pair<List<String>, List<ApplierInfoDto>> getApplierInfo(List<? extends Instance> appliers) {
        return getInstanceInfo(ApplierInfoInquirer.getInstance(), appliers);
    }


    public Pair<List<String>, List<ReplicatorInfoDto>> getReplicatorInfo(List<? extends Instance> replicators) {
        return getInstanceInfo(ReplicatorInfoInquirer.getInstance(), replicators);
    }


    public <T extends InfoDto> Pair<List<String>, List<T>> getInstanceInfo(AbstractInfoInquirer<T> infoInquirer, List<? extends Instance> instances) {
        String name = infoInquirer.name();
        List<Pair<String, Integer>> ipAndPorts = instances.stream()
                .map(instance -> Pair.from(instance.getIp(), instance.getPort()))
                .distinct().collect(Collectors.toList());
        List<Future<List<T>>> futures = Lists.newArrayList();
        for (Pair<String, Integer> pair : ipAndPorts) {
            futures.add(infoInquirer.query(pair.getKey() + ":" + pair.getValue()));
        }
        List<T> list = Lists.newArrayList();
        List<String> validIps = Lists.newArrayList();
        for (int i = 0; i < futures.size(); i++) {
            Future<List<T>> future = futures.get(i);
            Pair<String, Integer> pair = ipAndPorts.get(i);
            String ip = pair.getKey();
            Integer port = pair.getValue();
            try {
                List<T> infoDtos = future.get(1000, TimeUnit.MILLISECONDS);
                infoDtos.forEach(e -> {
                    e.setIp(ip);
                    e.setPort(port);
                });
                list.addAll(infoDtos);
                validIps.add(ip);
                EventMonitor.DEFAULT.logEvent(String.format("drc.inquiry.%s.success", name), ip + ":" + port);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                QUERY_INFO_LOGGER.error("get {} fail, skip for: {}", name, pair.getKey() + ":" + pair.getValue());
                EventMonitor.DEFAULT.logEvent(String.format("drc.inquiry.%s.fail", name), ip + ":" + port);
            }
        }
        return Pair.from(validIps, list);
    }
}
