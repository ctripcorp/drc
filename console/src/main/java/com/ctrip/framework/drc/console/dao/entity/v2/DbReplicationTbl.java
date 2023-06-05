package com.ctrip.framework.drc.console.dao.entity.v2;

import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * @author dql邓权亮
 * @date 2023-05-25
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "db_replication_tbl")
public class DbReplicationTbl implements DalPojo {

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

    /**
     * 源逻辑表
     */
    @Column(name = "src_logic_table_name")
    @Type(value = Types.VARCHAR)
    private String srcLogicTableName;

    /**
     * 目标逻辑表
     */
    @Column(name = "dst_logic_table_name")
    @Type(value = Types.VARCHAR)
    private String dstLogicTableName;

    @Override
    public String toString() {
        return "DbReplicationTbl{" +
                "id=" + id +
                ", srcMhaDbMappingId=" + srcMhaDbMappingId +
                ", dstMhaDbMappingId=" + dstMhaDbMappingId +
                ", replicationType=" + replicationType +
                ", deleted=" + deleted +
                ", createTime=" + createTime +
                ", datachangeLasttime=" + datachangeLasttime +
                ", srcLogicTableName='" + srcLogicTableName + '\'' +
                ", dstLogicTableName='" + dstLogicTableName + '\'' +
                '}';
    }

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

    public String getSrcLogicTableName() {
        return srcLogicTableName;
    }

    public void setSrcLogicTableName(String srcLogicTableName) {
        this.srcLogicTableName = srcLogicTableName;
    }

    public String getDstLogicTableName() {
        return dstLogicTableName;
    }

    public void setDstLogicTableName(String dstLogicTableName) {
        this.dstLogicTableName = dstLogicTableName;
    }

}
