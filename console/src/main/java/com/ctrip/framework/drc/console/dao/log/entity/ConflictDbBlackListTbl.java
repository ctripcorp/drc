package com.ctrip.framework.drc.console.dao.log.entity;

import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Created by dengquanliang
 * 2023/11/15 18:34
 */
@Entity
@Database(name = "bbzfxdrclogdb_w")
@Table(name = "conflict_db_blacklist_tbl")
public class ConflictDbBlackListTbl {
    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * db正则表达式
     */
    @Column(name = "db_filter")
    @Type(value = Types.VARCHAR)
    private String dbFilter;

    /**
     * 黑名单类型，0-用户添加,1-DRC系统自动添加
     * default: 0
     */
    @Column(name = "type")
    @Type(value = Types.TINYINT)
    private Integer type;

    /**
     * 更新时间
     */
    @Column(name = "datachange_lasttime", insertable = false, updatable = false)
    @Type(value = Types.TIMESTAMP)
    private Timestamp datachangeLasttime;

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

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getDbFilter() {
        return dbFilter;
    }

    public void setDbFilter(String dbFilter) {
        this.dbFilter = dbFilter;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Timestamp getDatachangeLasttime() {
        return datachangeLasttime;
    }

    public void setDatachangeLasttime(Timestamp datachangeLasttime) {
        this.datachangeLasttime = datachangeLasttime;
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
}
