package com.ctrip.framework.drc.console.dao.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Sensitive;
import com.ctrip.platform.dal.dao.annotation.Type;
import java.sql.Types;
import java.sql.Timestamp;

import com.ctrip.platform.dal.dao.DalPojo;

/**
 * @author wjx王继欣
 * @date 2021-02-21
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "data_inconsistency_history_tbl")
public class DataInconsistencyHistoryTbl implements DalPojo {

    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * 监控的库名
     */
    @Column(name = "monitor_schema_name")
    @Type(value = Types.VARCHAR)
    private String monitorSchemaName;

    /**
     * 监控的表名
     */
    @Column(name = "monitor_table_name")
    @Type(value = Types.VARCHAR)
    private String monitorTableName;

    /**
     * 监控的表key
     */
    @Column(name = "monitor_table_key")
    @Type(value = Types.VARCHAR)
    private String monitorTableKey;

    /**
     * 监控表不一致数值对应key的值
     */
    @Column(name = "monitor_table_key_value")
    @Type(value = Types.VARCHAR)
    private String monitorTableKeyValue;

    /**
     * 集群mha group id
     */
    @Column(name = "mha_group_id")
    @Type(value = Types.BIGINT)
    private Long mhaGroupId;

    /**
     * 存储来源,1:增量; 2:全量
     */
    @Column(name = "source_type")
    @Type(value = Types.TINYINT)
    private Integer sourceType;

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

    public String getMonitorSchemaName() {
        return monitorSchemaName;
    }

    public void setMonitorSchemaName(String monitorSchemaName) {
        this.monitorSchemaName = monitorSchemaName;
    }

    public String getMonitorTableName() {
        return monitorTableName;
    }

    public void setMonitorTableName(String monitorTableName) {
        this.monitorTableName = monitorTableName;
    }

    public String getMonitorTableKey() {
        return monitorTableKey;
    }

    public void setMonitorTableKey(String monitorTableKey) {
        this.monitorTableKey = monitorTableKey;
    }

    public String getMonitorTableKeyValue() {
        return monitorTableKeyValue;
    }

    public void setMonitorTableKeyValue(String monitorTableKeyValue) {
        this.monitorTableKeyValue = monitorTableKeyValue;
    }

    public Long getMhaGroupId() {
        return mhaGroupId;
    }

    public void setMhaGroupId(Long mhaGroupId) {
        this.mhaGroupId = mhaGroupId;
    }

    public Integer getSourceType() {
        return sourceType;
    }

    public void setSourceType(Integer sourceType) {
        this.sourceType = sourceType;
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
