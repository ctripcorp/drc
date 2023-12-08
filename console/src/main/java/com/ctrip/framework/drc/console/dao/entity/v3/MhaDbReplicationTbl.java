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
@Table(name = "mha_db_replication_tbl")
public class MhaDbReplicationTbl implements DalPojo {

    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * 源mha_db映射id
     */
    @Column(name = "src_mha_db_mapping_id")
    @Type(value = Types.BIGINT)
    private Long srcMhaDbMappingId;

    /**
     * 目标端mha_db映射id
     */
    @Column(name = "dst_mha_db_mapping_id")
    @Type(value = Types.BIGINT)
    private Long dstMhaDbMappingId;

    /**
     * 复制类型 0:db-db 1-db-mq
     */
    @Column(name = "replication_type")
    @Type(value = Types.TINYINT)
    private Integer replicationType;

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

    public Long getSrcMhaDbMappingId() {
        return srcMhaDbMappingId;
    }

    public void setSrcMhaDbMappingId(Long srcMhaDbMappingId) {
        this.srcMhaDbMappingId = srcMhaDbMappingId;
    }

    public Long getDstMhaDbMappingId() {
        return dstMhaDbMappingId;
    }

    public void setDstMhaDbMappingId(Long dstMhaDbMappingId) {
        this.dstMhaDbMappingId = dstMhaDbMappingId;
    }

    public Integer getReplicationType() {
        return replicationType;
    }

    public void setReplicationType(Integer replicationType) {
        this.replicationType = replicationType;
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
