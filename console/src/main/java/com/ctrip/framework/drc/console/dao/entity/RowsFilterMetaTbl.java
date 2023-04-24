package com.ctrip.framework.drc.console.dao.entity;

import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Created by dengquanliang
 * 2023/4/23 18:19
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "rows_filter_meta_tbl")
public class RowsFilterMetaTbl implements DalPojo {

    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * 行过滤唯一标识
     */
    @Column(name = "meta_filter_name")
    @Type(value = Types.VARCHAR)
    private String metaFilterName;

    /**
     * 行过滤类型, 1-黑名单 2-白名单
     */
    @Column(name = "filter_type")
    @Type(value = Types.TINYINT)
    private Integer filterType;

    /**
     * 部门
     */
    @Column(name = "bu")
    @Type(value = Types.VARCHAR)
    private String bu;

    /**
     * 负责人
     */
    @Column(name = "owner")
    @Type(value = Types.VARCHAR)
    private String owner;

    /**
     * 源机房
     */
    @Column(name = "src_region")
    @Type(value = Types.VARCHAR)
    private String srcRegion;

    /**
     * 目的机房
     */
    @Column(name = "des_region")
    @Type(value = Types.VARCHAR)
    private String desRegion;

    /**
     * token
     */
    @Column(name = "token")
    @Type(value = Types.VARCHAR)
    private String token;

    /**
     * 是否删除, 0-否 1-是
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

    public String getMetaFilterName() {
        return metaFilterName;
    }

    public void setMetaFilterName(String metaFilterName) {
        this.metaFilterName = metaFilterName;
    }

    public Integer getFilterType() {
        return filterType;
    }

    public void setFilterType(Integer filterType) {
        this.filterType = filterType;
    }

    public String getBu() {
        return bu;
    }

    public void setBu(String bu) {
        this.bu = bu;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getSrcRegion() {
        return srcRegion;
    }

    public void setSrcRegion(String srcRegion) {
        this.srcRegion = srcRegion;
    }

    public String getDesRegion() {
        return desRegion;
    }

    public void setDesRegion(String desRegion) {
        this.desRegion = desRegion;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
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
