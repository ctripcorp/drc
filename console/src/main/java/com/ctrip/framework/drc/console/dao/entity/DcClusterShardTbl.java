package com.ctrip.framework.drc.console.dao.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;
import java.sql.Types;
import java.sql.Timestamp;

import com.ctrip.platform.dal.dao.DalPojo;

/**
 * @author wjx王继欣
 * @date 2020-01-20
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "dc_cluster_shard_tbl")
public class DcClusterShardTbl implements DalPojo {

    /**
     * 空
     */
    @Id
    @Column(name = "dc_cluster_shard_id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long dcClusterShardId;

    /**
     * 空
     */
    @Column(name = "dc_cluster_id")
    @Type(value = Types.BIGINT)
    private Long dcClusterId;

    /**
     * 空
     */
    @Column(name = "shard_id")
    @Type(value = Types.BIGINT)
    private Long shardId;

    /**
     * 空
     */
    @Column(name = "datachange_lasttime", insertable = false, updatable = false)
    @Type(value = Types.TIMESTAMP)
    private Timestamp datachangeLasttime;

    public Long getDcClusterShardId() {
        return dcClusterShardId;
    }

    public void setDcClusterShardId(Long dcClusterShardId) {
        this.dcClusterShardId = dcClusterShardId;
    }

    public Long getDcClusterId() {
        return dcClusterId;
    }

    public void setDcClusterId(Long dcClusterId) {
        this.dcClusterId = dcClusterId;
    }

    public Long getShardId() {
        return shardId;
    }

    public void setShardId(Long shardId) {
        this.shardId = shardId;
    }

    public Timestamp getDatachangeLasttime() {
        return datachangeLasttime;
    }

    public void setDatachangeLasttime(Timestamp datachangeLasttime) {
        this.datachangeLasttime = datachangeLasttime;
    }

}
