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
@Table(name = "db_cluster_tbl")
public class DbClusterTbl implements DalPojo {

    /**
     * 空
     */
    @Id
    @Column(name = "db_cluster_id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long dbClusterId;

    /**
     * 空
     */
    @Column(name = "read_user")
    @Type(value = Types.VARCHAR)
    private String readUser;

    /**
     * 空
     */
    @Column(name = "read_password")
    @Type(value = Types.VARCHAR)
    private String readPassword;

    /**
     * 空
     */
    @Column(name = "write_user")
    @Type(value = Types.VARCHAR)
    private String writeUser;

    /**
     * 空
     */
    @Column(name = "write_password")
    @Type(value = Types.VARCHAR)
    private String writePassword;

    /**
     * 空
     */
    @Column(name = "monitor_user")
    @Type(value = Types.VARCHAR)
    private String monitorUser;

    /**
     * 空
     */
    @Column(name = "monitor_password")
    @Type(value = Types.VARCHAR)
    private String monitorPassword;

    /**
     * 空
     */
    @Column(name = "dc_cluster_id")
    @Type(value = Types.BIGINT)
    private Long dcClusterId;

    /**
     * 空
     */
    @Column(name = "datachange_lasttime", insertable = false, updatable = false)
    @Type(value = Types.TIMESTAMP)
    private Timestamp datachangeLasttime;

    public Long getDbClusterId() {
        return dbClusterId;
    }

    public void setDbClusterId(Long dbClusterId) {
        this.dbClusterId = dbClusterId;
    }

    public String getReadUser() {
        return readUser;
    }

    public void setReadUser(String readUser) {
        this.readUser = readUser;
    }

    public String getReadPassword() {
        return readPassword;
    }

    public void setReadPassword(String readPassword) {
        this.readPassword = readPassword;
    }

    public String getWriteUser() {
        return writeUser;
    }

    public void setWriteUser(String writeUser) {
        this.writeUser = writeUser;
    }

    public String getWritePassword() {
        return writePassword;
    }

    public void setWritePassword(String writePassword) {
        this.writePassword = writePassword;
    }

    public String getMonitorUser() {
        return monitorUser;
    }

    public void setMonitorUser(String monitorUser) {
        this.monitorUser = monitorUser;
    }

    public String getMonitorPassword() {
        return monitorPassword;
    }

    public void setMonitorPassword(String monitorPassword) {
        this.monitorPassword = monitorPassword;
    }

    public Long getDcClusterId() {
        return dcClusterId;
    }

    public void setDcClusterId(Long dcClusterId) {
        this.dcClusterId = dcClusterId;
    }

    public Timestamp getDatachangeLasttime() {
        return datachangeLasttime;
    }

    public void setDatachangeLasttime(Timestamp datachangeLasttime) {
        this.datachangeLasttime = datachangeLasttime;
    }

}
