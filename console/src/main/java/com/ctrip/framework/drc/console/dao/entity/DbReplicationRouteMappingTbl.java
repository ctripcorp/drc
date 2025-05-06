package com.ctrip.framework.drc.console.dao.entity;

import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Created by dengquanliang
 * 2025/4/1 14:28
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "db_replication_route_mapping_tbl")
public class DbReplicationRouteMappingTbl {
    /**
     * primary key
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * mhaDbReplicationId
     */
    @Column(name = "mha_db_replication_id")
    @Type(value = Types.BIGINT)
    private Long mhaDbReplicationId;

    /**
     * mhaId
     */
    @Column(name = "mha_id")
    @Type(value = Types.BIGINT)
    private Long mhaId;

    /**
     * route_id
     */
    @Column(name = "route_id")
    @Type(value = Types.BIGINT)
    private Long routeId;

    /**
     * deleted or not
     */
    @Column(name = "deleted")
    @Type(value = Types.TINYINT)
    private Integer deleted;

    /**
     * create time
     */
    @Column(name = "create_time")
    @Type(value = Types.TIMESTAMP)
    private Timestamp createTime;

    /**
     * data changed last time
     */
    @Column(name = "datachange_lasttime", insertable = false, updatable = false)
    @Type(value = Types.TIMESTAMP)
    private Timestamp datachangeLasttime;

    public Long getMhaId() {
        return mhaId;
    }

    public void setMhaId(Long mhaId) {
        this.mhaId = mhaId;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getMhaDbReplicationId() {
        return mhaDbReplicationId;
    }

    public void setMhaDbReplicationId(Long mhaDbReplicationId) {
        this.mhaDbReplicationId = mhaDbReplicationId;
    }

    public Long getRouteId() {
        return routeId;
    }

    public void setRouteId(Long routeId) {
        this.routeId = routeId;
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
