package com.ctrip.framework.drc.core.server.config.replicator.dto;

import com.ctrip.framework.drc.core.server.config.MonitorConfig;
import java.util.List;

/**
 * @ClassName ReplicatorConfigDtoV2
 * @Author haodongPan
 * @Date 2024/8/6 18:50
 * @Version: $  DbDto replace the Db in ReplicatorConfigDto
 */
public class ReplicatorConfigDtoV2 extends MonitorConfig {

    private DbDto master;

    private List<String> uuids;

    private List<String> tableNames;

    private int applierPort;  //for applier

    private String gtidSet;

    private String clusterName;

    private String mhaName;

    private String readUser;

    private String readPassward;

    private int status;

    private String previousMaster;

    private int applyMode;


    public DbDto getMaster() {
        return master;
    }

    public void setMaster(DbDto master) {
        this.master = master;
    }

    public List<String> getUuids() {
        return uuids;
    }

    public void setUuids(List<String> uuids) {
        this.uuids = uuids;
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public void setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
    }

    public int getApplierPort() {
        return applierPort;
    }

    public void setApplierPort(int applierPort) {
        this.applierPort = applierPort;
    }

    public String getGtidSet() {
        return gtidSet;
    }

    public void setGtidSet(String gtidSet) {
        this.gtidSet = gtidSet;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public String getReadUser() {
        return readUser;
    }

    public void setReadUser(String readUser) {
        this.readUser = readUser;
    }

    public String getReadPassward() {
        return readPassward;
    }

    public void setReadPassward(String readPassward) {
        this.readPassward = readPassward;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getPreviousMaster() {
        return previousMaster;
    }

    public void setPreviousMaster(String previousMaster) {
        this.previousMaster = previousMaster;
    }

    public int getApplyMode() {
        return applyMode;
    }

    public void setApplyMode(int applyMode) {
        this.applyMode = applyMode;
    }
}