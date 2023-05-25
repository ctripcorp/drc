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
@Table(name = "mha_tbl_v2")
public class MhaTblV2 implements DalPojo {

    /**
     * 主键
     */
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Type(value = Types.BIGINT)
    private Long id;

    /**
     * mha名称
     */
    @Column(name = "mha_name")
    @Type(value = Types.VARCHAR)
    private String mhaName;

    /**
     * 机房id
     */
    @Column(name = "dc_id")
    @Type(value = Types.BIGINT)
    private Long dcId;

    /**
     * bu名
     */
    @Column(name = "bu_name")
    @Type(value = Types.VARCHAR)
    private String buName;

    /**
     * 集群名称，兼容老版
     */
    @Column(name = "cluster_name")
    @Type(value = Types.VARCHAR)
    private String clusterName;

    /**
     * binlog拉取账号用户名
     */
    @Column(name = "read_user")
    @Type(value = Types.VARCHAR)
    private String readUser;

    /**
     * binlog拉取账号密码
     */
    @Column(name = "read_password")
    @Type(value = Types.VARCHAR)
    private String readPassword;

    /**
     * 写账号用户名
     */
    @Column(name = "write_user")
    @Type(value = Types.VARCHAR)
    private String writeUser;

    /**
     * 写账号密码
     */
    @Column(name = "write_password")
    @Type(value = Types.VARCHAR)
    private String writePassword;

    /**
     * 监控账号用户名
     */
    @Column(name = "monitor_user")
    @Type(value = Types.VARCHAR)
    private String monitorUser;

    /**
     * 监控账号密码
     */
    @Column(name = "monitor_password")
    @Type(value = Types.VARCHAR)
    private String monitorPassword;

    /**
     * 是否开启监控, 0-否; 1-是
     */
    @Column(name = "monitor_switch")
    @Type(value = Types.TINYINT)
    private Integer monitorSwitch;

    /**
     * apply mode,0-set_gtid 1-transaction_table(default)
     */
    @Column(name = "apply_mode")
    @Type(value = Types.TINYINT)
    private Integer applyMode;

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

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public Long getDcId() {
        return dcId;
    }

    public void setDcId(Long dcId) {
        this.dcId = dcId;
    }

    public String getBuName() {
        return buName;
    }

    public void setBuName(String buName) {
        this.buName = buName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
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

    public Integer getMonitorSwitch() {
        return monitorSwitch;
    }

    public void setMonitorSwitch(Integer monitorSwitch) {
        this.monitorSwitch = monitorSwitch;
    }

    public Integer getApplyMode() {
        return applyMode;
    }

    public void setApplyMode(Integer applyMode) {
        this.applyMode = applyMode;
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
