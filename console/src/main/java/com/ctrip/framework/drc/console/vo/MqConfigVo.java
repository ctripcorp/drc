package com.ctrip.framework.drc.console.vo;

import com.ctrip.framework.drc.console.dao.entity.DataMediaPairTbl;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;

import static com.ctrip.framework.drc.console.enums.DataMediaPairTypeEnum.DB_TO_MQ;

/**
 * @ClassName MqConfigVo
 * @Author haodongPan
 * @Date 2022/10/26 15:05
 * @Version: $
 */
public class MqConfigVo {
    
    //dataMediaPairId
    private long id;
    // qmq/kafka
    private String mqType;
    //schema.table
    private String table;
    // mq topic
    private String topic;
    //json/arvo
    private String serialization;
    // qmq store message when send fail
    private boolean persistent;
    // titankey/dalCluster
    private String persistentDb;
    // partition order
    private boolean order;
    // orderKey / partition key
    private String orderKey;
    // qmq send message delayTime, unit:second
    private long delayTime;
    // for otter eventProcessor
    private String processor;


    public static MqConfigVo from(DataMediaPairTbl dataMediaPairTbl) {
        if (!DB_TO_MQ.getType().equals(dataMediaPairTbl.getType())) {
            throw new IllegalArgumentException("error dataMediaPair type for mqConfig");
        }

        
        MqConfigVo vo = new MqConfigVo();
        
        vo.setId(dataMediaPairTbl.getId());
        vo.setTable(dataMediaPairTbl.getSrcDataMediaName());
        vo.setTopic(dataMediaPairTbl.getDestDataMediaName());
        
        MqConfig mqConfig = JsonUtils.fromJson(dataMediaPairTbl.getProperties(), MqConfig.class);
        vo.setMqType(mqConfig.getMqType());
        vo.setSerialization(mqConfig.getSerialization());
        vo.setPersistent(mqConfig.isPersistent());
        vo.setPersistentDb(mqConfig.getPersistentDb());
        vo.setOrder(mqConfig.isOrder());
        vo.setOrderKey(mqConfig.getOrderKey());
        vo.setDelayTime(mqConfig.getDelayTime());
        vo.setProcessor(mqConfig.getProcessor());

        return vo;
        
    }

    public MqConfigVo() {
    }

    @Override
    public String toString() {
        return "MqConfigVo{" +
                "id=" + id +
                ", mqType='" + mqType + '\'' +
                ", table='" + table + '\'' +
                ", topic='" + topic + '\'' +
                ", serialization='" + serialization + '\'' +
                ", persistent=" + persistent +
                ", persistentDb='" + persistentDb + '\'' +
                ", order=" + order +
                ", orderKey='" + orderKey + '\'' +
                ", delayTime=" + delayTime +
                ", processor='" + processor + '\'' +
                '}';
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getMqType() {
        return mqType;
    }

    public void setMqType(String mqType) {
        this.mqType = mqType;
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

    public String getSerialization() {
        return serialization;
    }

    public void setSerialization(String serialization) {
        this.serialization = serialization;
    }

    public boolean isPersistent() {
        return persistent;
    }

    public void setPersistent(boolean persistent) {
        this.persistent = persistent;
    }

    public String getPersistentDb() {
        return persistentDb;
    }

    public void setPersistentDb(String persistentDb) {
        this.persistentDb = persistentDb;
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

    public long getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(long delayTime) {
        this.delayTime = delayTime;
    }

    public String getProcessor() {
        return processor;
    }

    public void setProcessor(String processor) {
        this.processor = processor;
    }
}
