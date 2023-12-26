package com.ctrip.framework.drc.console.dao.entity.v3;

import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * @author zyn郑永念
 * @date 2023-11-14
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "messenger_group_tbl_v3")
public class MessengerGroupTblV3 implements DalPojo {

    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * mha_db_replication_id
     */
    @Column(name = "mha_db_replication_id")
    @Type(value = Types.BIGINT)
    private Long mhaDbReplicationId;

    /**
     * 已经执行的gtid
     */
    @Column(name = "gtid_executed")
    @Type(value = Types.LONGVARCHAR)
    private String gtidExecuted;

    /**
     * 创建时间
     */
    @Column(name = "create_time")
    @Type(value = Types.TIMESTAMP)
    private Timestamp createTime;

    /**
     * 是否删除, 0:否; 1:是
     */
    @Column(name = "deleted")
    @Type(value = Types.TINYINT)
    private Integer deleted;

    /**
     * 更新时间
     */
    @Column(name = "datachange_lasttime", insertable = false, updatable = false)
    @Type(value = Types.TIMESTAMP)
    private Timestamp datachangeLasttime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getMhaDbReplicationId() {
        return mhaDbReplicationId;
    }

    public void setMhaDbReplicationId(Long mhaDbReplicationId) {
        this.mhaDbReplicationId = mhaDbReplicationId;
    }

    public String getGtidExecuted() {
        return gtidExecuted;
    }

    public void setGtidExecuted(String gtidExecuted) {
        this.gtidExecuted = gtidExecuted;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }

    public Integer getDeleted() {
        return deleted;
    }

    public void setDeleted(Integer deleted) {
        this.deleted = deleted;
    }

    public Timestamp getDatachangeLasttime() {
        return datachangeLasttime;
    }

    public void setDatachangeLasttime(Timestamp datachangeLasttime) {
        this.datachangeLasttime = datachangeLasttime;
    }

}