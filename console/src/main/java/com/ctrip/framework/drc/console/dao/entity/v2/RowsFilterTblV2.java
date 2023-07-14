package com.ctrip.framework.drc.console.dao.entity.v2;

import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Objects;

/**
 * Created by dengquanliang
 * 2023/6/25 16:39
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "rows_filter_tbl_v2")
public class RowsFilterTblV2 {
    /**
     * pk
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * 列过滤配置 模式 0-java_regex 1-trip_udl 2-trip_uid
     */
    @Column(name = "mode")
    @Type(value = Types.TINYINT)
    private Integer mode;

    /**
     * 列
     */
    @Column(name = "configs")
    @Type(value = Types.LONGVARCHAR)
    private String configs;

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

    public Integer getMode() {
        return mode;
    }

    public void setMode(Integer mode) {
        this.mode = mode;
    }

    public String getConfigs() {
        return configs;
    }

    public void setConfigs(String configs) {
        this.configs = configs;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowsFilterTblV2 that = (RowsFilterTblV2) o;
        return Objects.equals(mode, that.mode) && Objects.equals(configs, that.configs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mode, configs);
    }

    @Override
    public String toString() {
        return "RowsFilterTblV2{" +
                "id=" + id +
                ", mode=" + mode +
                ", configs='" + configs + '\'' +
                ", deleted=" + deleted +
                ", createTime=" + createTime +
                ", datachangeLasttime=" + datachangeLasttime +
                '}';
    }
}
