package com.ctrip.framework.drc.console.dao.entity.v3;


import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * @author zyn郑永念
 * @date 2023-10-24
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "applier_group_tbl_v3")
public class ApplierGroupTblV3 implements DalPojo {

    /**
     * primary key
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * mha_replication_id
     */
    @Column(name = "mha_db_replication_id")
    @Type(value = Types.BIGINT)
    private Long mhaDbReplicationId;

    /**
     * 初始同步位点
     */
    @Column(name = "gtid_init")
    @Type(value = Types.LONGVARCHAR)
    private String gtidInit;

    /**
     * apply 并发数
     */
    @Column(name = "concurrency")
    @Type(value = Types.INTEGER)
    private Integer concurrency;

    /**
     * 是否删除, 0-否; 1-是
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

    public String getGtidInit() {
        return gtidInit;
    }

    public void setGtidInit(String gtidInit) {
        this.gtidInit = gtidInit;
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

    public Integer getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(Integer concurrency) {
        this.concurrency = concurrency;
    }
}
