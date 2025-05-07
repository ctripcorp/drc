package com.ctrip.framework.drc.core.server.config.applier.dto;

import com.ctrip.framework.drc.core.meta.ApplierMeta;
import com.ctrip.framework.drc.core.server.config.ApplierRegistryKey;
import com.ctrip.framework.drc.core.utils.NameUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * Created by shiruixin
 * 2024/11/20 16:44
 */
public abstract class FetcherConfigDto extends ApplierMeta {
    protected int gaqSize;
    protected int workerCount;
    protected int workerSize;
    protected String gtidExecuted;
    protected String managerIp;
    protected int managerPort;
    protected String includedDbs;
    protected String nameFilter;
    protected String nameMapping;
    protected String routeInfo;
    protected String skipEvent;
    protected int applyMode;
    protected String properties;

    protected int applyConcurrency;

    public String getManagerIp() {
        return managerIp;
    }

    public void setManagerIp(String managerIp) {
        this.managerIp = managerIp;
    }

    public int getManagerPort() {
        return managerPort;
    }

    public void setManagerPort(int managerPort) {
        this.managerPort = managerPort;
    }

    public String getGtidExecuted() {
        return gtidExecuted;
    }

    public void setGtidExecuted(String gtidExecuted) {
        this.gtidExecuted = gtidExecuted;
    }

    public int getGaqSize() {
        return gaqSize;
    }

    public void setGaqSize(int gaqSize) {
        this.gaqSize = gaqSize;
    }

    public int getWorkerCount() {
        return workerCount;
    }

    public void setWorkerCount(int workerCount) {
        this.workerCount = workerCount;
    }

    public int getWorkerSize() {
        return workerSize;
    }

    public void setWorkerSize(int workerSize) {
        this.workerSize = workerSize;
    }

    public String getIncludedDbs() {
        return includedDbs;
    }

    public void setIncludedDbs(String includedDbs) {
        this.includedDbs = includedDbs;
    }

    public String getNameFilter() {
        return nameFilter;
    }

    public void setNameFilter(String nameFilter) {
        this.nameFilter = nameFilter;
    }

    public String getNameMapping() {
        return nameMapping;
    }

    public void setNameMapping(String nameMapping) {
        this.nameMapping = nameMapping;
    }

    public String getRouteInfo() {
        return routeInfo;
    }

    public void setRouteInfo(String routeInfo) {
        this.routeInfo = routeInfo;
    }

    public String getSkipEvent() {
        return skipEvent;
    }

    public void setSkipEvent(String skipEvent) {
        this.skipEvent = skipEvent;
    }

    public int getApplyMode() {
        return applyMode;
    }

    public void setApplyMode(int applyMode) {
        this.applyMode = applyMode;
    }

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }

    public int getApplyConcurrency() {
        return applyConcurrency;
    }

    public void setApplyConcurrency(int applyConcurrency) {
        this.applyConcurrency = applyConcurrency;
    }

    @JsonIgnore
    public String getRegistryKey() {
        String registerKey = ApplierRegistryKey.from(target.mhaName, super.getCluster(), replicator.mhaName);
        registerKey = NameUtils.dotSchemaIfNeed(registerKey, applyMode, includedDbs);
        return registerKey;
    }


    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), gaqSize, workerCount, workerSize, replicator.ip, replicator.port, target.ip, includedDbs, nameFilter, nameMapping, routeInfo, target.port, applyMode, properties);
    }

    abstract public boolean equalsIgnoreProperties(Object o);

    abstract public boolean equalsProperties(Object o);
}
