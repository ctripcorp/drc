package com.ctrip.framework.drc.console.dao.log.entity;

import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Created by dengquanliang
 * 2023/10/30 19:59
 */
@Entity
@Database(name = "bbzfxdrclogdb_w")
@Table(name = "conflict_auto_handle_tbl")
public class ConflictAutoHandleTbl {
    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * 批次id
     */
    @Column(name = "batch_id")
    @Type(value = Types.BIGINT)
    private Long batchId;

    /**
     * 行记录id
     */
    @Column(name = "row_log_id")
    @Type(value = Types.BIGINT)
    private Long rowLogId;

    /**
     * 自动冲突处理sql
     */
    @Column(name = "auto_handle_sql")
    @Type(value = Types.VARCHAR)
    private String autoHandleSql;

    /**
     * 执行结果 0-执行失败 1-执行成功
     */
    @Column(name = "result")
    @Type(value = Types.TINYINT)
    private Integer result;

    /**
     * 备注,失败原因
     */
    @Column(name = "remark")
    @Type(value = Types.VARCHAR)
    private String remark;

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

    public Long getBatchId() {
        return batchId;
    }

    public void setBatchId(Long batchId) {
        this.batchId = batchId;
    }

    public Long getRowLogId() {
        return rowLogId;
    }

    public void setRowLogId(Long rowLogId) {
        this.rowLogId = rowLogId;
    }

    public String getAutoHandleSql() {
        return autoHandleSql;
    }

    public void setAutoHandleSql(String autoHandleSql) {
        this.autoHandleSql = autoHandleSql;
    }

    public Integer getResult() {
        return result;
    }

    public void setResult(Integer result) {
        this.result = result;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
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
