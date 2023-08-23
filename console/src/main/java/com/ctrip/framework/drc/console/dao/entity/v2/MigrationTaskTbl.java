package com.ctrip.framework.drc.console.dao.entity.v2;

import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * @author phd潘昊栋
 * @date 2023-08-22
 */
@Entity
@Database(name = "fxdrcmetadb_dalcluster")
@Table(name = "migration_task_tbl")
public class MigrationTaskTbl implements DalPojo {

    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * 搬迁db
     */
    @Column(name = "dbs")
    @Type(value = Types.LONGVARCHAR)
    private String dbs;

    /**
     * 原集群名(DRC系统）
     */
    @Column(name = "old_mha")
    @Type(value = Types.VARCHAR)
    private String oldMha;

    /**
     * 新集群名(DRC系统)
     */
    @Column(name = "new_mha")
    @Type(value = Types.VARCHAR)
    private String newMha;

    /**
     * 原集群名(DBA系统）
     */
    @Column(name = "old_mha_dba")
    @Type(value = Types.VARCHAR)
    private String oldMhaDba;

    /**
     * 新集群名(DBA系统）
     */
    @Column(name = "new_mha_dba")
    @Type(value = Types.VARCHAR)
    private String newMhaDba;

    /**
     * 任务状态
     */
    @Column(name = "status")
    @Type(value = Types.VARCHAR)
    private String status;

    /**
     * 操作者
     */
    @Column(name = "operator")
    @Type(value = Types.VARCHAR)
    private String operator;

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
     * 修改时间
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

    public String getDbs() {
        return dbs;
    }

    public void setDbs(String dbs) {
        this.dbs = dbs;
    }

    public String getOldMha() {
        return oldMha;
    }

    public void setOldMha(String oldMha) {
        this.oldMha = oldMha;
    }

    public String getNewMha() {
        return newMha;
    }

    public void setNewMha(String newMha) {
        this.newMha = newMha;
    }

    public String getOldMhaDba() {
        return oldMhaDba;
    }

    public void setOldMhaDba(String oldMhaDba) {
        this.oldMhaDba = oldMhaDba;
    }

    public String getNewMhaDba() {
        return newMhaDba;
    }

    public void setNewMhaDba(String newMhaDba) {
        this.newMhaDba = newMhaDba;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
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
