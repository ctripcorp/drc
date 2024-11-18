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
@Table(name = "mha_db_mapping_tbl")
public class MhaDbMappingTbl implements DalPojo {

    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * mha_id
     */
    @Column(name = "mha_id")
    @Type(value = Types.BIGINT)
    private Long mhaId;

    /**
     * db_id
     */
    @Column(name = "db_id")
    @Type(value = Types.BIGINT)
    private Long dbId;

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
     * 子环境
     */
    @Column(name = "subenv")
    @Type(value = Types.VARCHAR)
    private String subenv;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getMhaId() {
        return mhaId;
    }

    public void setMhaId(Long mhaId) {
        this.mhaId = mhaId;
    }

    public Long getDbId() {
        return dbId;
    }

    public void setDbId(Long dbId) {
        this.dbId = dbId;
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

    public String getSubenv() {
        return subenv;
    }

    public void setSubenv(String subenv) {
        this.subenv = subenv;
    }
}
