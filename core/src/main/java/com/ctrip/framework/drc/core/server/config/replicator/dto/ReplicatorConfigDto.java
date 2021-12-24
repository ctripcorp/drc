package com.ctrip.framework.drc.core.server.config.replicator.dto;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.Db;
import com.ctrip.framework.drc.core.server.config.MonitorConfig;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Created by mingdongli
 * 2019/10/30 上午10:07.
 */
public class ReplicatorConfigDto extends MonitorConfig {

    private static Logger logger = LoggerFactory.getLogger(ReplicatorConfigDto.class);

    private Db master;

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

    public Db getMaster() {
        return master;
    }

    public void setMaster(Db master) {
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

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public ReplicatorConfig toReplicatorConfig() {
        ReplicatorConfig replicatorConfig = new ReplicatorConfig();
        replicatorConfig.setRegistryKey(getClusterName(), getMhaName());
        replicatorConfig.setApplierPort(getApplierPort());
        if (gtidSet != null && gtidSet != "") {
            replicatorConfig.setGtidSet(new GtidSet(gtidSet.trim()));
        }
        Set<UUID> uuidSet = Sets.newHashSet();
        for (String uuid : uuids) {
            if (StringUtils.isBlank(uuid)) {
                logger.warn("[Blank] uuid receive");
                continue;
            }
            uuidSet.add(UUID.fromString(uuid));
        }
        replicatorConfig.setWhiteUUID(uuidSet);
        if (tableNames != null) {
            replicatorConfig.setTableNames(new HashSet<>(getTableNames()));
        }
        Endpoint endpoint = new DefaultEndPoint(master.getIp(), master.getPort(), getReadUser(), getReadPassward());
        replicatorConfig.setEndpoint(endpoint);
        replicatorConfig.setStatus(getStatus());
        replicatorConfig.setPreviousMaster(getPreviousMaster());

        replicatorConfig.setMonitorConfig(new MonitorConfig(getClusterAppId(), getBu(), getSrcDcName()));

        return replicatorConfig;
    }

    @Override
    public String toString() {
        return "ReplicatorConfigDto{" +
                "master=" + master +
                ", uuids=" + uuids +
                ", tableNames=" + tableNames +
                ", applierPort=" + applierPort +
                ", gtidSet='" + gtidSet + '\'' +
                ", clusterName='" + clusterName + '\'' +
                ", mhaName='" + mhaName + '\'' +
                ", readUser='" + readUser + '\'' +
                ", readPassward='" + readPassward + '\'' +
                ", status=" + status +
                ", previousMaster='" + previousMaster + '\'' +
                "} " + super.toString();
    }
}


