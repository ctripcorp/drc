package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.aop.forward.PossibleRemote;
import com.ctrip.framework.drc.console.config.ConsoleConfig;
import com.ctrip.framework.drc.console.service.v2.RemoteResourceService;
import com.ctrip.framework.drc.core.entity.Instance;
import com.ctrip.framework.drc.core.entity.SimpleInstance;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierInfoDto;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorInfoDto;
import com.ctrip.framework.drc.core.service.inquirer.BatchInfoInquirer;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author yongnian
 * @create 2024/12/17 13:36
 */
@Service
public class RemoteResourceServiceImpl implements RemoteResourceService {
    private BatchInfoInquirer batchInfoInquirer = BatchInfoInquirer.getInstance();

    @Override
    @PossibleRemote(path = "/api/drc/v2/remote/resource/dbApplierInstances")
    public Map<String, List<Instance>> getCurrentDbApplierInstances(String src, String mhaName, List<String> ips) {
        List<ApplierInfoDto> applierInfoDtos = queryApplierInfo(mhaName, ips);
        return applierInfoDtos.stream()
                .filter(e -> {
                    String srcMha = RegistryKey.getTargetMha(e.getRegistryKey());
                    return Objects.equals(srcMha, src);
                })
                .collect(Collectors.groupingBy(e -> Objects.requireNonNull(RegistryKey.getTargetDB(e.getRegistryKey()))
                        , Collectors.mapping(SimpleInstance::from, Collectors.toList())
                ));
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/remote/resource/replicatorInstances")
    public List<Instance> getCurrentReplicatorInstance(String mhaName, List<String> ips) {
        List<ReplicatorInfoDto> replicatorInfoDtos = queryReplicatorInfo(mhaName, ips);
        return replicatorInfoDtos.stream().map(SimpleInstance::from).collect(Collectors.toList());
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/remote/resource/messengerInstances")
    public List<Instance> getCurrentMessengerInstance(String mhaName, List<String> ips) {
        List<ApplierInfoDto> applierInfoDtos = queryMessengerInfo(mhaName, ips);
        return applierInfoDtos.stream().map(SimpleInstance::from).collect(Collectors.toList());
    }

    public List<ApplierInfoDto> queryApplierInfo(String mha, List<String> ips) {
        List<SimpleInstance> instances = ips.stream()
                .map(e -> SimpleInstance.from(e, ConsoleConfig.DEFAULT_APPLIER_PORT))
                .collect(Collectors.toList());
        Pair<List<String>, List<ApplierInfoDto>> pair = batchInfoInquirer.getApplierInfo(instances);
        return pair.getValue().stream().filter(e -> {
            String dst = RegistryKey.from(e.getRegistryKey()).getMhaName();
            return Objects.equals(mha, dst);
        }).collect(Collectors.toList());
    }

    public List<ApplierInfoDto> queryMessengerInfo(String mha, List<String> ips) {
        List<SimpleInstance> instances = ips.stream()
                .map(e -> SimpleInstance.from(e, ConsoleConfig.DEFAULT_APPLIER_PORT))
                .collect(Collectors.toList());
        Pair<List<String>, List<ApplierInfoDto>> pair = batchInfoInquirer.getMessengerInfo(instances);
        List<ApplierInfoDto> infoDtos = pair.getValue();
        return infoDtos.stream().filter(e -> {
            String mhaName = RegistryKey.from(e.getRegistryKey()).getMhaName();
            return Objects.equals(mhaName, mha);
        }).collect(Collectors.toList());
    }

    public List<ReplicatorInfoDto> queryReplicatorInfo(String mha, List<String> ips) {
        List<SimpleInstance> instances = ips.stream()
                .map(e -> SimpleInstance.from(e, ConsoleConfig.DEFAULT_APPLIER_PORT))
                .collect(Collectors.toList());
        Pair<List<String>, List<ReplicatorInfoDto>> pair = batchInfoInquirer.getReplicatorInfo(instances);
        List<ReplicatorInfoDto> infoDtos = pair.getValue();
        return infoDtos.stream().filter(e -> {
            String registryKey = e.getRegistryKey();
            String mhaName = RegistryKey.from(registryKey).getMhaName();
            return Objects.equals(mhaName, mha);
        }).collect(Collectors.toList());
    }


}
