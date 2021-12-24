package com.ctrip.framework.drc.core.driver.config;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.google.common.collect.Sets;

import java.util.Set;
import java.util.UUID;

/**
 * Created by mingdongli
 * 2019/9/21 上午11:07.
 */
public class MySQLSlaveConfig extends Identity implements GlobalConfig {

    private long slaveId;

    private GtidSet gtidSet;

    private String previousMaster;

    private int binlogChecksum = BINLOG_CHECKSUM_ALG_OFF;

    private Set<UUID> uuidSet = Sets.newConcurrentHashSet();

    private Set<String> tableNames = Sets.newConcurrentHashSet();

    private int instanceStatus;

    public long getSlaveId() {
        return slaveId;
    }

    public void setSlaveId(long slaveId) {
        this.slaveId = slaveId;
    }

    public GtidSet getGtidSet() {
        return gtidSet;
    }

    public void setGtidSet(GtidSet gtidSet) {
        this.gtidSet = gtidSet;
    }

    public int getBinlogChecksum() {
        return binlogChecksum;
    }

    public void setBinlogChecksum(int binlogChecksum) {
        this.binlogChecksum = binlogChecksum;
    }

    public Set<UUID> getUuidSet() {
        return uuidSet;
    }

    public void setUuidSet(Set<UUID> uuidSet) {
        this.uuidSet = uuidSet;
    }

    public Set<String> getTableNames() {
        return tableNames;
    }

    public void setTableNames(Set<String> tableNames) {
        this.tableNames = tableNames;
    }

    public String getPreviousMaster() {
        return previousMaster;
    }

    public void setPreviousMaster(String previousMaster) {
        this.previousMaster = previousMaster;
    }

    public void setInstanceStatus(int instanceStatus) {
        this.instanceStatus = instanceStatus;
    }

    public boolean isMaster() {
        return InstanceStatus.ACTIVE.getStatus() == instanceStatus;
    }

    @Override
    public String toString() {
        return "MySQLSlaveConfig{" +
                "slaveId=" + slaveId +
                ", gtidSet=" + gtidSet +
                ", previousMaster='" + previousMaster + '\'' +
                ", binlogChecksum=" + binlogChecksum +
                ", uuidSet=" + uuidSet +
                ", tableNames=" + tableNames +
                ", instanceStatus=" + instanceStatus +
                "} " + super.toString();
    }
}
