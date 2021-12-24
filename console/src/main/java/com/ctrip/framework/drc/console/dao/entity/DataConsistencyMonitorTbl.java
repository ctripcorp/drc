package com.ctrip.framework.drc.console.dao.entity;

import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * @author wjx王继欣
 * @date 2020-12-29
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "data_consistency_monitor_tbl")
public class DataConsistencyMonitorTbl implements DalPojo {

    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.INTEGER)
    private Integer id;

    /**
     * 需要监控的mha_id
     */
    @Column(name = "mha_id")
    @Type(value = Types.INTEGER)
    private Integer mhaId;

    /**
     * 需要监控的数据库名
     */
    @Column(name = "monitor_schema_name")
    @Type(value = Types.VARCHAR)
    private String monitorSchemaName;

    /**
     * 需要监控的表名
     */
    @Column(name = "monitor_table_name")
    @Type(value = Types.VARCHAR)
    private String monitorTableName;

    /**
     * 需要监控的表key
     */
    @Column(name = "monitor_table_key")
    @Type(value = Types.VARCHAR)
    private String monitorTableKey;

    /**
     * 需要监控的表onUpdate字段
     */
    @Column(name = "monitor_table_on_update")
    @Type(value = Types.VARCHAR)
    private String monitorTableOnUpdate;

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
     * 是否监控, 0:否; 1:是
     */
    @Column(name = "monitor_switch")
    @Type(value = Types.TINYINT)
    private Integer monitorSwitch;

    /**
     * 全量数据一致性校验状态，0:未校验；1:校验中；2:校验完 3:校验失败
     */
    @Column(name = "full_data_check_status")
    @Type(value = Types.TINYINT)
    private Integer fullDataCheckStatus;

    /**
     * 最近全量数据一致性校验时间
     */
    @Column(name = "full_data_check_lasttime")
    @Type(value = Types.TIMESTAMP)
    private Timestamp fullDataCheckLasttime;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getMhaId() {
        return mhaId;
    }

    public void setMhaId(Integer mhaId) {
        this.mhaId = mhaId;
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

    public String getMonitorTableOnUpdate() {
        return monitorTableOnUpdate;
    }

    public void setMonitorTableOnUpdate(String monitorTableOnUpdate) {
        this.monitorTableOnUpdate = monitorTableOnUpdate;
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

    public Integer getMonitorSwitch() {
        return monitorSwitch;
    }

    public void setMonitorSwitch(Integer monitorSwitch) {
        this.monitorSwitch = monitorSwitch;
    }

    public Integer getFullDataCheckStatus() {
        return fullDataCheckStatus;
    }

    public void setFullDataCheckStatus(Integer fullDataCheckStatus) {
        this.fullDataCheckStatus = fullDataCheckStatus;
    }

    public Timestamp getFullDataCheckLasttime() {
        return fullDataCheckLasttime;
    }

    public void setFullDataCheckLasttime(Timestamp fullDataCheckLasttime) {
        this.fullDataCheckLasttime = fullDataCheckLasttime;
    }

    public String getMonitorTableSchema() {
        return getMonitorSchemaName() + "." + getMonitorTableName();
    }

}
