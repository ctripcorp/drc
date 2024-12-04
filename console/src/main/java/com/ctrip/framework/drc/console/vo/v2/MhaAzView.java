package com.ctrip.framework.drc.console.vo.v2;

import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierInfoDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.FetcherInfoDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerInfoDto;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by shiruixin
 * 2024/9/6 15:23
 */
public class MhaAzView {
    Map<String, Set<String>> az2mhaName;
    Map<String, List<String>> az2DbInstance;
    Map<String, List<String>> az2ReplicatorInstance;
    Map<String, List<ApplierInfoDto>> az2ApplierInstance;
    Map<String, List<MessengerInfoDto>> az2MessengerInstance;
    Map<String, Set<String>> az2DrcDb;


    public Map<String, Set<String>> getAz2mhaName() {
        return az2mhaName;
    }

    public void setAz2mhaName(Map<String, Set<String>> az2mhaName) {
        this.az2mhaName = az2mhaName;
    }

    public Map<String, List<String>> getAz2DbInstance() {
        return az2DbInstance;
    }

    public void setAz2DbInstance(Map<String, List<String>> az2DbInstance) {
        this.az2DbInstance = az2DbInstance;
    }

    public Map<String, List<String>> getAz2ReplicatorInstance() {
        return az2ReplicatorInstance;
    }

    public void setAz2ReplicatorInstance(Map<String, List<String>> az2ReplicatorInstance) {
        this.az2ReplicatorInstance = az2ReplicatorInstance;
    }

    public Map<String, List<ApplierInfoDto>> getAz2ApplierInstance() {
        return az2ApplierInstance;
    }

    public void setAz2ApplierInstance(Map<String, List<? extends FetcherInfoDto>> azFetcherInstance) {
        this.az2ApplierInstance = Maps.newHashMap();
        azFetcherInstance.forEach((key, value) -> {
            List<ApplierInfoDto> infoDtos = value.stream()
                    .filter(ApplierInfoDto.class::isInstance)
                    .map(ApplierInfoDto.class::cast)
                    .collect(Collectors.toList());
            this.az2ApplierInstance.put(key, infoDtos);
        });
    }

    public Map<String, List<MessengerInfoDto>> getAz2MessengerInstance() {
        return az2MessengerInstance;
    }

    public void setAz2MessengerInstance(Map<String, List<? extends FetcherInfoDto>> azFetcherInstance) {
        this.az2MessengerInstance = Maps.newHashMap();
        azFetcherInstance.forEach((key, value) -> {
            List<MessengerInfoDto> infoDtos = value.stream()
                    .filter(MessengerInfoDto.class::isInstance)
                    .map(MessengerInfoDto.class::cast)
                    .collect(Collectors.toList());
            this.az2MessengerInstance.put(key, infoDtos);
        });
    }

    public Map<String, Set<String>> getAz2DrcDb() {
        return az2DrcDb;
    }

    public void setAz2DrcDb(Map<String, Set<String>> az2DrcDb) {
        this.az2DrcDb = az2DrcDb;
    }
}
