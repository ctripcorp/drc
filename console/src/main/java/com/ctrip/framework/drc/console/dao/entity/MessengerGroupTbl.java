package com.ctrip.framework.drc.console.dao.entity;

import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * @ClassName MessengerGroupTbl
 * @Author haodongPan
 * @Date 2022/9/30 14:20
 * @Version: $
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "messenger_group_tbl")
public class MessengerGroupTbl implements DalPojo{

    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * mha_id
     */
    @Column(name = "mha_id")
    @Type(value = Types.BIGINT)
    private Long mhaId;

    /**
     * replicator group id
     */
    @Column(name = "replicator_group_id")
    @Type(value = Types.BIGINT)
    private Long replicatorGroupId;

    /**
     * mq_type
     * @see MqType
     */
    @Column(name = "mq_type")
    @Type(value = Types.VARCHAR)
    private String mqType;

    /**
     * gtid_executed
     */
    @Column(name = "gtid_executed")
    @Type(value = Types.VARCHAR)
    private String gtidExecuted;  
    
    
    /**
     * 是否删除, 0:否; 1:是
     */
    @Column(name = "deleted")
    @Type(value = Types.TINYINT)
    private Integer deleted;

    /**
     * 创建时间
     */
    @Column(name = "create_time")
    @Type(value = Types.TIMESTAMP)
    private Timestamp createTime;

    /**
     * 更新时间
     */
    @Column(name = "datachange_lasttime", insertable = false, updatable = false)
    @Type(value = Types.TIMESTAMP)
    private Timestamp datachangeLasttime;


    public String getMqType() {
        return mqType;
    }

    public void setMqType(String mqType) {
        this.mqType = mqType;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getMhaId() {
        return mhaId;
    }

    public void setMhaId(Long mhaId) {
        this.mhaId = mhaId;
    }

    public Long getReplicatorGroupId() {
        return replicatorGroupId;
    }

    public void setReplicatorGroupId(Long replicatorGroupId) {
        this.replicatorGroupId = replicatorGroupId;
    }

    public String getGtidExecuted() {
        return gtidExecuted;
    }

    public void setGtidExecuted(String gtidExecuted) {
        this.gtidExecuted = gtidExecuted;
    }

    public Integer getDeleted() {
        return deleted;
    }

    public void setDeleted(Integer deleted) {
        this.deleted = deleted;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }

    public Timestamp getDatachangeLasttime() {
        return datachangeLasttime;
    }

    public void setDatachangeLasttime(Timestamp datachangeLasttime) {
        this.datachangeLasttime = datachangeLasttime;
    }
}
