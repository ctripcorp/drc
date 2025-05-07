package com.ctrip.framework.drc.console.dto.v3;

import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildReq;
import com.ctrip.framework.drc.console.vo.v2.MqMetaCreateResultView;
import com.ctrip.framework.drc.core.mq.MqType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by shiruixin
 * 2025/4/3 15:10
 */
public class MqAutoCreateDto extends DbMqCreateDto {
    private final Logger autoConfigLogger = LoggerFactory.getLogger("autoConfig");

    String dbName;
    String kafkaCluster;
    Integer partitions;
    Integer maxMessageMB;

    public Logger getAutoConfigLogger() {
        return autoConfigLogger;
    }

    public String getKafkaCluster() {
        return kafkaCluster;
    }

    public void setKafkaCluster(String kafkaCluster) {
        this.kafkaCluster = kafkaCluster;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public DrcAutoBuildReq deriveDrcAutoBuildReq(DrcAutoBuildReq.BuildMode buildMode) {
        MqType type = MqType.valueOf(mqConfig.getMqType());
        DrcAutoBuildReq dto = new DrcAutoBuildReq();
        dto.setMode(buildMode.getValue());
        switch (buildMode) {
            case DAL_CLUSTER_NAME:
                dto.setDbName(dbName);
                dto.setDalClusterName("");
                break;
            case MULTI_DB_NAME:
                dto.setDbName(String.join(",", dbNames));
                dto.setSrcRegionName(srcRegionName);
                dto.setReplicationType(type.getReplicationType().getType());
                dto.setTblsFilterDetail(new DrcAutoBuildReq.TblsFilterDetail(logicTableConfig.getLogicTable()));
                break;
        }

        return dto;
    }

    public MhaDbReplicationCreateDto deriveMhaDbReplicationCreateDto() {
        MhaDbReplicationCreateDto dto = new MhaDbReplicationCreateDto();
        dto.setSrcRegionName(srcRegionName);
        dto.setDbName(dbName);
        MqType type = MqType.valueOf(mqConfig.getMqType());
        dto.setReplicationType(type.getReplicationType().getType());
        return dto;
    }

    public MqMetaCreateResultView deriveMqMetaCreateResultView() {
        MqMetaCreateResultView view = new MqMetaCreateResultView();
        view.setDbs(dbNames);
        view.setRegion(srcRegionName);
        view.setTable(logicTableConfig.getLogicTable());
        view.setTopic(logicTableConfig.getDstLogicTable());
        return view;
    }

    public Integer getPartitions() {
        return partitions;
    }

    public void setPartitions(Integer partitions) {
        this.partitions = partitions;
    }

    public Integer getMaxMessageMB() {
        return maxMessageMB;
    }

    public void setMaxMessageMB(Integer maxMessageMB) {
        this.maxMessageMB = maxMessageMB;
    }
}
