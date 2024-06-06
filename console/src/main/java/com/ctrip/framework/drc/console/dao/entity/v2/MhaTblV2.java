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
     * buId
     */
    @Column(name = "bu_id")
    @Type(value = Types.BIGINT)
    private Long buId;

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
     * binlog拉取账号密码加密Token
     */
    @Column(name = "read_password_token")
    @Type(value = Types.VARCHAR)
    private String readPasswordToken;
    
    /**
     * binlog拉取账号用户名V2
     */
    @Column(name = "read_user_v2")
    @Type(value = Types.VARCHAR)
    private String readUserV2;

    /**
     * binlog拉取账号密码加密TokenV2
     */
    @Column(name = "read_password_token_v2")
    @Type(value = Types.VARCHAR)
    private String readPasswordTokenV2;
    

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
     * 写账号用密码加密Token
     */
    @Column(name = "write_password_token")
    @Type(value = Types.VARCHAR)
    private String writePasswordToken;

    /**
     * 写账号用户名V2
     */
    @Column(name = "write_user_v2")
    @Type(value = Types.VARCHAR)
    private String writeUserV2;

    /**
     * 写账号用密码加密TokenV2
     */
    @Column(name = "write_password_token_v2")
    @Type(value = Types.VARCHAR)
    private String writePasswordTokenV2;

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
     * 监控账号密码加密token
     */
    @Column(name = "monitor_password_token")
    @Type(value = Types.VARCHAR)
    private String monitorPasswordToken;


    /**
     * 监控账号用户名V2
     */
    @Column(name = "monitor_user_v2")
    @Type(value = Types.VARCHAR)
    private String monitorUserV2;

    /**
     * 监控账号密码加密tokenv2
     */
    @Column(name = "monitor_password_token_v2")
    @Type(value = Types.VARCHAR)
    private String monitorPasswordTokenV2;
    
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

    /**
     * appId
     */
    @Column(name = "app_id")
    @Type(value = Types.BIGINT)
    private Long appId;

    /**
     * 标签
     */
    @Column(name = "tag")
    @Type(value = Types.VARCHAR)
    private String tag;

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

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

    public Long getBuId() {
        return buId;
    }

    public void setBuId(Long buId) {
        this.buId = buId;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Deprecated
    public String getReadUser() {
        return readUser;
    }

    @Deprecated
    public void setReadUser(String readUser) {
        this.readUser = readUser;
    }

    @Deprecated
    public String getReadPassword() {
        return readPassword;
    }

    @Deprecated
    public void setReadPassword(String readPassword) {
        this.readPassword = readPassword;
    }

    @Deprecated
    public String getWriteUser() {
        return writeUser;
    }

    @Deprecated
    public void setWriteUser(String writeUser) {
        this.writeUser = writeUser;
    }

    @Deprecated
    public String getWritePassword() {
        return writePassword;
    }

    @Deprecated
    public void setWritePassword(String writePassword) {
        this.writePassword = writePassword;
    }

    @Deprecated
    public String getMonitorUser() {
        return monitorUser;
    }

    @Deprecated
    public void setMonitorUser(String monitorUser) {
        this.monitorUser = monitorUser;
    }

    @Deprecated
    public String getMonitorPassword() {
        return monitorPassword;
    }

    @Deprecated
    public void setMonitorPassword(String monitorPassword) {
        this.monitorPassword = monitorPassword;
    }


    public String getReadPasswordToken() {
        return readPasswordToken;
    }

    public void setReadPasswordToken(String readPasswordToken) {
        this.readPasswordToken = readPasswordToken;
    }

    public String getReadUserV2() {
        return readUserV2;
    }

    public void setReadUserV2(String readUserV2) {
        this.readUserV2 = readUserV2;
    }

    public String getReadPasswordTokenV2() {
        return readPasswordTokenV2;
    }

    public void setReadPasswordTokenV2(String readPasswordTokenV2) {
        this.readPasswordTokenV2 = readPasswordTokenV2;
    }

    public String getWritePasswordToken() {
        return writePasswordToken;
    }

    public void setWritePasswordToken(String writePasswordToken) {
        this.writePasswordToken = writePasswordToken;
    }

    public String getWriteUserV2() {
        return writeUserV2;
    }

    public void setWriteUserV2(String writeUserV2) {
        this.writeUserV2 = writeUserV2;
    }

    public String getWritePasswordTokenV2() {
        return writePasswordTokenV2;
    }

    public void setWritePasswordTokenV2(String writePasswordTokenV2) {
        this.writePasswordTokenV2 = writePasswordTokenV2;
    }

    public String getMonitorPasswordToken() {
        return monitorPasswordToken;
    }

    public void setMonitorPasswordToken(String monitorPasswordToken) {
        this.monitorPasswordToken = monitorPasswordToken;
    }

    public String getMonitorUserV2() {
        return monitorUserV2;
    }

    public void setMonitorUserV2(String monitorUserV2) {
        this.monitorUserV2 = monitorUserV2;
    }

    public String getMonitorPasswordTokenV2() {
        return monitorPasswordTokenV2;
    }

    public void setMonitorPasswordTokenV2(String monitorPasswordTokenV2) {
        this.monitorPasswordTokenV2 = monitorPasswordTokenV2;
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

    public Long getAppId() {
        return appId;
    }

    public void setAppId(Long appId) {
        this.appId = appId;
    }

    @Override
    public String toString() {
        return "MhaTblV2{" +
                "id=" + id +
                ", mhaName='" + mhaName + '\'' +
                ", dcId=" + dcId +
                ", buId=" + buId +
                ", clusterName='" + clusterName + '\'' +
                ", readUser='" + readUser + '\'' +
                ", readPassword='" + readPassword + '\'' +
                ", readPasswordToken='" + readPasswordToken + '\'' +
                ", readUserV2='" + readUserV2 + '\'' +
                ", readPasswordTokenV2='" + readPasswordTokenV2 + '\'' +
                ", writeUser='" + writeUser + '\'' +
                ", writePassword='" + writePassword + '\'' +
                ", writePasswordToken='" + writePasswordToken + '\'' +
                ", writeUserV2='" + writeUserV2 + '\'' +
                ", writePasswordTokenV2='" + writePasswordTokenV2 + '\'' +
                ", monitorUser='" + monitorUser + '\'' +
                ", monitorPassword='" + monitorPassword + '\'' +
                ", monitorPasswordToken='" + monitorPasswordToken + '\'' +
                ", monitorUserV2='" + monitorUserV2 + '\'' +
                ", monitorPasswordTokenV2='" + monitorPasswordTokenV2 + '\'' +
                ", monitorSwitch=" + monitorSwitch +
                ", applyMode=" + applyMode +
                ", deleted=" + deleted +
                ", createTime=" + createTime +
                ", datachangeLasttime=" + datachangeLasttime +
                ", appId=" + appId +
                ", tag='" + tag + '\'' +
                '}';
    }
}
