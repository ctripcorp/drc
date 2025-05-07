package com.ctrip.framework.drc.console.dto.v3;

import com.ctrip.framework.drc.console.dto.v2.MqConfigDto;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.core.mq.MqType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by shiruixin
 * 2025/4/23 17:46
 */
public class MqAutoCreateRequestDto {
    private final Logger autoConfigLogger = LoggerFactory.getLogger("autoConfig");

    String dbName;
    String region;
    String bu;
    String mqType;
    boolean order;
    String orderKey;
    String table;
    String topic;
    String kafkaCluster;
    Integer partitions;
    Integer maxMessageMB;

    public void check() {
        if (StringUtils.isBlank(table)
                || StringUtils.isBlank(topic)
                || StringUtils.isBlank(mqType)
                || StringUtils.isBlank(region)
                || StringUtils.isBlank(dbName)
                || StringUtils.isBlank(bu)) {
            autoConfigLogger.error("[[tag=autoconfig]] empty params");
            throw ConsoleExceptionUtils.message("empty params");
        }
        dbName = dbName.trim().toLowerCase();
        region = region.trim().toLowerCase();
        MqType type = MqType.valueOf(mqType);
        if (MqType.qmq == type && !topic.startsWith(bu + ".")) {
            autoConfigLogger.error("[[tag=autoconfig]] wrong qmq topic (should start with bu name)");
            throw ConsoleExceptionUtils.message("wrong qmq topic (should start with bu name)");
        }

        if (MqType.qmq == type && order) {
            autoConfigLogger.error("[[tag=autoconfig]] not support auto config order qmq topic");
            throw ConsoleExceptionUtils.message("not support auto config order qmq topic");
        }

        if (!order && !StringUtils.isBlank(orderKey)) {
            autoConfigLogger.error("[[tag=autoconfig]] wrong order or orderKey");
            throw ConsoleExceptionUtils.message("wrong order or orderKey");
        }

        if (MqType.kafka == type && StringUtils.isBlank(kafkaCluster)) {
            autoConfigLogger.error("[[tag=autoconfig]] empty param kafkaCluster");
            throw ConsoleExceptionUtils.message("empty param kafkaCluster");
        }
    }

    public MqAutoCreateDto deriveMqAutoCreateDto() {
        MqAutoCreateDto dto = new MqAutoCreateDto();
        MqConfigDto mqConfigDto = new MqConfigDto();
        LogicTableConfig logicTableConfig = new LogicTableConfig();
        dto.setMqConfig(mqConfigDto);
        dto.setLogicTableConfig(logicTableConfig);

        dto.setDbName(dbName);
        dto.setSrcRegionName(region);
        dto.setKafkaCluster(kafkaCluster);
        dto.setPartitions(partitions);
        dto.setMaxMessageMB(maxMessageMB);

        mqConfigDto.setBu(bu);
        mqConfigDto.setMqType(mqType);
        mqConfigDto.setOrder(order);
        mqConfigDto.setOrderKey(orderKey);
        mqConfigDto.setSerialization("json");
        mqConfigDto.setPersistent(false);

        logicTableConfig.setLogicTable(table);
        logicTableConfig.setDstLogicTable(topic);
        return dto;
    }


    @Override
    public String toString() {
        return "MqAutoCreateRequestDto{" +
                "dbName='" + dbName + '\'' +
                ", region='" + region + '\'' +
                ", bu='" + bu + '\'' +
                ", mqType='" + mqType + '\'' +
                ", order=" + order +
                ", orderKey='" + orderKey + '\'' +
                ", table='" + table + '\'' +
                ", topic='" + topic + '\'' +
                ", kafkaCluster='" + kafkaCluster + '\'' +
                ", partitions=" + partitions +
                ", maxMessageMB=" + maxMessageMB +
                '}';
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getBu() {
        return bu;
    }

    public void setBu(String bu) {
        this.bu = bu;
    }

    public String getMqType() {
        return mqType;
    }

    public void setMqType(String mqType) {
        this.mqType = mqType;
    }

    public boolean isOrder() {
        return order;
    }

    public void setOrder(boolean order) {
        this.order = order;
    }

    public String getOrderKey() {
        return orderKey;
    }

    public void setOrderKey(String orderKey) {
        this.orderKey = orderKey;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getKafkaCluster() {
        return kafkaCluster;
    }

    public void setKafkaCluster(String kafkaCluster) {
        this.kafkaCluster = kafkaCluster;
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
