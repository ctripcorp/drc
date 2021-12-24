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
@Table(name = "zk_server_tbl")
public class ZkServerTbl implements DalPojo {

    /**
     * 空
     */
    @Id
    @Column(name = "zk_server_id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long zkServerId;

    /**
     * 空
     */
    @Column(name = "zk_server_ip")
    @Type(value = Types.VARCHAR)
    private String zkServerIp;

    /**
     * 空
     */
    @Column(name = "zk_server_port")
    @Type(value = Types.INTEGER)
    private Integer zkServerPort;

    /**
     * 空
     */
    @Column(name = "dc_id")
    @Type(value = Types.BIGINT)
    private Long dcId;

    /**
     * 空
     */
    @Column(name = "datachange_lasttime", insertable = false, updatable = false)
    @Type(value = Types.TIMESTAMP)
    private Timestamp datachangeLasttime;

    public Long getZkServerId() {
        return zkServerId;
    }

    public void setZkServerId(Long zkServerId) {
        this.zkServerId = zkServerId;
    }

    public String getZkServerIp() {
        return zkServerIp;
    }

    public void setZkServerIp(String zkServerIp) {
        this.zkServerIp = zkServerIp;
    }

    public Integer getZkServerPort() {
        return zkServerPort;
    }

    public void setZkServerPort(Integer zkServerPort) {
        this.zkServerPort = zkServerPort;
    }

    public Long getDcId() {
        return dcId;
    }

    public void setDcId(Long dcId) {
        this.dcId = dcId;
    }

    public Timestamp getDatachangeLasttime() {
        return datachangeLasttime;
    }

    public void setDatachangeLasttime(Timestamp datachangeLasttime) {
        this.datachangeLasttime = datachangeLasttime;
    }

}

