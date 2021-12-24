package com.ctrip.framework.drc.core.server.config.validation.dto;

import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ValidationConfigDto extends InstanceInfo {

    private String gtidExecuted;

    public InstanceInfo replicator;

    private List<DBInfo> machines;

    private Map<String, String> uidMap;

    private Map<String, Integer> ucsStrategyIdMap;

    public Map<String, Integer> getUcsStrategyIdMap() {
        return ucsStrategyIdMap;
    }

    public void setUcsStrategyIdMap(Map<String, Integer> ucsStrategyIdMap) {
        this.ucsStrategyIdMap = ucsStrategyIdMap;
    }

    public Map<String, String> getUidMap() {
        return uidMap;
    }

    public void setUidMap(Map<String, String> uidMap) {
        this.uidMap = uidMap;
    }

    public String getGtidExecuted() {
        return gtidExecuted;
    }

    public void setGtidExecuted(String gtidExecuted) {
        this.gtidExecuted = gtidExecuted;
    }

    public InstanceInfo getReplicator() {
        return replicator;
    }

    public void setReplicator(InstanceInfo replicator) {
        this.replicator = replicator;
    }

    public List<DBInfo> getMachines() {
        return machines;
    }

    public void setMachines(List<DBInfo> machines) {
        this.machines = machines;
    }

    @JsonIgnore
    public String getRegistryKey() {
        return RegistryKey.from(super.getCluster(), super.getMhaName());
    }

    @Override
    public String toString() {
        return "ValidationConfigDto{" +
                "name='" + name + '\'' +
                ", mhaName='" + mhaName + '\'' +
                ", port=" + port +
                ", ip='" + ip + '\'' +
                ", idc='" + idc + '\'' +
                ", cluster='" + cluster + '\'' +
                ", gtidExecuted='" + gtidExecuted + '\'' +
                ", replicator=" + replicator +
                ", machines=" + machines +
                ", uidMap=" + uidMap +
                ", ucsStrategyIdMap=" + ucsStrategyIdMap +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ValidationConfigDto that = (ValidationConfigDto) o;
        return Objects.equals(replicator, that.replicator) && Objects.equals(machines, that.machines) && Objects.equals(uidMap, that.uidMap) && Objects.equals(ucsStrategyIdMap, that.ucsStrategyIdMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), gtidExecuted, replicator, machines, uidMap, ucsStrategyIdMap);
    }
}
