package com.ctrip.framework.drc.core.server.config.replicator;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.config.GlobalConfig;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.ctrip.framework.drc.core.monitor.entity.BaseEndpointEntity;
import com.ctrip.framework.drc.core.monitor.entity.TrafficEntity;
import com.ctrip.framework.drc.core.monitor.enums.DirectionEnum;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.server.config.MonitorConfig;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;
import java.util.UUID;

/**
 * Created by mingdongli
 * 2019/9/22 下午5:06
 */
public class ReplicatorConfig implements GlobalConfig {

    private int status;

    private MySQLMasterConfig mySQLMasterConfig = new MySQLMasterConfig();

    private MySQLSlaveConfig mySQLSlaveConfig = new MySQLSlaveConfig();

    private MonitorConfig monitorConfig = new MonitorConfig();

    public MySQLSlaveConfig getMySQLSlaveConfig() {
        return mySQLSlaveConfig;
    }

    public void setMySQLSlaveConfig(MySQLSlaveConfig mySQLSlaveConfig) {
        this.mySQLSlaveConfig = mySQLSlaveConfig;
    }

    public MySQLMasterConfig getMySQLMasterConfig() {
        return mySQLMasterConfig;
    }

    public void setMySQLMasterConfig(MySQLMasterConfig mySQLMasterConfig) {
        this.mySQLMasterConfig = mySQLMasterConfig;
    }

    public void setMonitorConfig(MonitorConfig monitorConfig) {
        this.monitorConfig = monitorConfig;
    }

    public String getIp() {
        return mySQLMasterConfig.getIp();
    }

    public void setIp(String ip) {
        mySQLMasterConfig.setIp(ip);
    }

    public int getApplierPort() {
        return mySQLMasterConfig.getPort();
    }

    public void setApplierPort(int port) {
        mySQLMasterConfig.setPort(port);
    }

    public Endpoint getEndpoint() {
        return mySQLSlaveConfig.getEndpoint();
    }

    public void setEndpoint(Endpoint endpoint) {
        mySQLSlaveConfig.setEndpoint(endpoint);
    }

    public String getRegistryKey() {
        return mySQLSlaveConfig.getRegistryKey();
    }

    public void setRegistryKey(String clusterName, String mhaName) {
        mySQLSlaveConfig.setRegistryKey(clusterName, mhaName);
    }

    public long getSlaveId() {
        return mySQLSlaveConfig.getSlaveId();
    }

    public void setSlaveId(long slaveId) {
        mySQLSlaveConfig.setSlaveId(slaveId);
    }

    public GtidSet getGtidSet() {
        return mySQLSlaveConfig.getGtidSet();
    }

    public void setGtidSet(GtidSet gtidSet) {
        mySQLSlaveConfig.setGtidSet(gtidSet);
    }

    public int getBinlogChecksum() {
        return mySQLSlaveConfig.getBinlogChecksum();
    }

    public void setBinlogChecksum(int binlogChecksum) {
        mySQLSlaveConfig.setBinlogChecksum(binlogChecksum);
    }

    public Set<String> getTableNames() {
        return mySQLSlaveConfig.getTableNames();
    }

    public void setTableNames(Set<String> tableNames) {
        mySQLSlaveConfig.setTableNames(tableNames);
    }

    public Set<UUID> getWhiteUUID() {
        return mySQLSlaveConfig.getUuidSet();
    }

    public void setWhiteUUID(Set<UUID> uuid) {
        mySQLSlaveConfig.setUuidSet(uuid);
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        mySQLSlaveConfig.setInstanceStatus(status);
        this.status = status;
    }

    public long getClusterAppId() {
        return monitorConfig.getClusterAppId();
    }

    public void setPreviousMaster(String previousMaster) {
        mySQLSlaveConfig.setPreviousMaster(previousMaster);
    }

    public TrafficEntity getTrafficEntity(DirectionEnum direction) {
        String ip = mySQLMasterConfig.getIp();
        if (StringUtils.isBlank(ip)) {
            ip = SystemConfig.LOCAL_SERVER_ADDRESS;
        }
        return new TrafficEntity.Builder()
                .clusterAppId(monitorConfig.getClusterAppId())
                .buName(monitorConfig.getBu())
                .dcName(monitorConfig.getSrcDcName())
                .clusterName(getRegistryKey())
                .ip(ip)
                .port(mySQLMasterConfig.getPort())
                .direction(direction.getDescription())
                .module(ModuleEnum.REPLICATOR.getDescription())
                .build();
    }

    public BaseEndpointEntity getBaseEndpointEntity() {
        String ip = mySQLMasterConfig.getIp();
        if (StringUtils.isBlank(ip)) {
            ip = SystemConfig.LOCAL_SERVER_ADDRESS;
        }
        return new BaseEndpointEntity.Builder()
                .clusterAppId(monitorConfig.getClusterAppId())
                .buName(monitorConfig.getBu())
                .dcName(monitorConfig.getSrcDcName())
                .clusterName(getRegistryKey())
                .ip(ip)
                .port(mySQLMasterConfig.getPort())
                .build();
    }
}
