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
@Table(name = "db_replication_filter_mapping_tbl")
public class DbReplicationFilterMappingTbl implements DalPojo {

    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * db_replication_id
     */
    @Column(name = "db_replication_id")
    @Type(value = Types.BIGINT)
    private Long dbReplicationId;

    /**
     * 过滤规则id
     */
    @Column(name = "rows_filter_id")
    @Type(value = Types.BIGINT)
    private Long rowsFilterId;

    /**
     * 列过滤规则id
     */
    @Column(name = "columns_filter_id")
    @Type(value = Types.BIGINT)
    private Long columnsFilterId;

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
     * messenger过滤规则id
     */
    @Column(name = "messenger_filter_id")
    @Type(value = Types.BIGINT)
    private Long messengerFilterId;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getDbReplicationId() {
        return dbReplicationId;
    }

    public void setDbReplicationId(Long dbReplicationId) {
        this.dbReplicationId = dbReplicationId;
    }

    public Long getRowsFilterId() {
        return rowsFilterId;
    }

    public void setRowsFilterId(Long rowsFilterId) {
        this.rowsFilterId = rowsFilterId;
    }

    public Long getColumnsFilterId() {
        return columnsFilterId;
    }

    public void setColumnsFilterId(Long columnsFilterId) {
        this.columnsFilterId = columnsFilterId;
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

    public Long getMessengerFilterId() {
        return messengerFilterId;
    }

    public void setMessengerFilterId(Long messengerFilterId) {
        this.messengerFilterId = messengerFilterId;
    }

    @Override
    public String toString() {
        return "DbReplicationFilterMappingTbl{" +
                "id=" + id +
                ", dbReplicationId=" + dbReplicationId +
                ", rowsFilterId=" + rowsFilterId +
                ", columnsFilterId=" + columnsFilterId +
                ", deleted=" + deleted +
                ", createTime=" + createTime +
                ", datachangeLasttime=" + datachangeLasttime +
                ", messengerFilterId=" + messengerFilterId +
                '}';
    }
}
