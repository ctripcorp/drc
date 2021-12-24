package com.ctrip.framework.drc.fetcher.activity.replicator.config;

import com.ctrip.framework.drc.core.driver.config.GlobalConfig;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;

/**
 * Created by mingdongli
 * 2019/9/24 上午11:21.
 */
public class FetcherSlaveConfig extends MySQLSlaveConfig implements GlobalConfig {

    private String applierName;

    private Set<String> includedDbs = Sets.newHashSet();

    private String nameFilter = StringUtils.EMPTY;

    public String getApplierName() {
        return applierName;
    }

    public void setApplierName(String applierName) {
        this.applierName = applierName;
    }

    public Set<String> getIncludedDbs() {
        return includedDbs;
    }

    public void setIncludedDbs(Set<String> includedDbs) {
        this.includedDbs = includedDbs;
    }

    public String getNameFilter() {
        return nameFilter;
    }

    public void setNameFilter(String nameFilter) {
        this.nameFilter = nameFilter;
    }

    @Override
    public String toString() {
        return "FetcherSlaveConfig{" +
                "endpoint=" + getEndpoint().getSocketAddress() +
                "registryKey=" + getRegistryKey() +
                "slaveId=" + getSlaveId() +
                ", gtidSet=" + getGtidSet() +
                ", previousMaster='" + getPreviousMaster() + '\'' +
                ", binlogChecksum=" + getBinlogChecksum() +
                ", uuidSet=" + getUuidSet() +
                ", includedDbs=" + getIncludedDbs() +
                ", applierName=" + getApplierName() +
                ", nameFilter=" + getNameFilter() +
                '}';
    }
}
