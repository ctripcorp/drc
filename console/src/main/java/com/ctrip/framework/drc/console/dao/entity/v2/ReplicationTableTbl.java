package com.ctrip.framework.drc.console.dao.entity.v2;

import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Objects;

/**
 * Created by dengquanliang
 * 2024/1/23 16:25
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "replication_table_tbl")
public class ReplicationTableTbl {
    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * 库名
     */
    @Column(name = "db_name")
    @Type(value = Types.VARCHAR)
    private String dbName;

    /**
     * 表名
     */
    @Column(name = "table_name")
    @Type(value = Types.VARCHAR)
    private String tableName;

    /**
     * 源region
     */
    @Column(name = "src_region")
    @Type(value = Types.VARCHAR)
    private String srcRegion;

    /**
     * 目标region
     */
    @Column(name = "dst_region")
    @Type(value = Types.VARCHAR)
    private String dstRegion;

    /**
     * 源mha
     */
    @Column(name = "src_mha")
    @Type(value = Types.VARCHAR)
    private String srcMha;

    /**
     * 目标mha
     */
    @Column(name = "dst_mha")
    @Type(value = Types.VARCHAR)
    private String dstMha;

    /**
     * db_replication_id
     */
    @Column(name = "db_replication_id")
    @Type(value = Types.BIGINT)
    private Long dbReplicationId;

    /**
     * 刷存量数据状态 0-未处理 1-处理中 2-处理完成 3-无需处理
     */
    @Column(name = "existing_data_status")
    @Type(value = Types.TINYINT)
    private Integer existingDataStatus;

    /**
     * 生效状态 0-未生效 1-生效中 2-已生效
     */
    @Column(name = "effective_status")
    @Type(value = Types.TINYINT)
    private Integer effectiveStatus;

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

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSrcRegion() {
        return srcRegion;
    }

    public void setSrcRegion(String srcRegion) {
        this.srcRegion = srcRegion;
    }

    public String getDstRegion() {
        return dstRegion;
    }

    public void setDstRegion(String dstRegion) {
        this.dstRegion = dstRegion;
    }

    public String getSrcMha() {
        return srcMha;
    }

    public void setSrcMha(String srcMha) {
        this.srcMha = srcMha;
    }

    public String getDstMha() {
        return dstMha;
    }

    public void setDstMha(String dstMha) {
        this.dstMha = dstMha;
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

    public Long getDbReplicationId() {
        return dbReplicationId;
    }

    public void setDbReplicationId(Long dbReplicationId) {
        this.dbReplicationId = dbReplicationId;
    }

    public Integer getExistingDataStatus() {
        return existingDataStatus;
    }

    public void setExistingDataStatus(Integer existingDataStatus) {
        this.existingDataStatus = existingDataStatus;
    }

    public Integer getEffectiveStatus() {
        return effectiveStatus;
    }

    public void setEffectiveStatus(Integer effectiveStatus) {
        this.effectiveStatus = effectiveStatus;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicationTableTbl that = (ReplicationTableTbl) o;
        return Objects.equals(dbName, that.dbName) && Objects.equals(tableName, that.tableName)
                && Objects.equals(srcRegion, that.srcRegion) && Objects.equals(dstRegion, that.dstRegion)
                && Objects.equals(srcMha, that.srcMha) && Objects.equals(dstMha, that.dstMha)
                && Objects.equals(dbReplicationId, that.dbReplicationId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName, tableName, srcRegion, dstRegion, srcMha, dstMha, dbReplicationId);
    }

    @Override
    public String toString() {
        return "ReplicationTableTbl{" +
                "id=" + id +
                ", dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", srcRegion='" + srcRegion + '\'' +
                ", dstRegion='" + dstRegion + '\'' +
                ", srcMha='" + srcMha + '\'' +
                ", dstMha='" + dstMha + '\'' +
                ", dbReplicationId=" + dbReplicationId +
                ", existingDataStatus=" + existingDataStatus +
                ", effectedStatus=" + effectiveStatus +
                ", deleted=" + deleted +
                ", createTime=" + createTime +
                ", datachangeLasttime=" + datachangeLasttime +
                '}';
    }
}
