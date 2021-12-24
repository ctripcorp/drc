package com.ctrip.framework.drc.console.dao.entity;

import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * @author shb沈海波
 * @date 2021-03-09
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "mha_group_tbl")
public class MhaGroupTbl implements DalPojo {

	/**
	 * 主键
	 */
	@Id
	@Column(name = "id")
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Type(value = Types.BIGINT)
	private Long id;

	/**
	 * DRC复制状态是否正常, 0:否; 1:是
	 */
	@Column(name = "drc_status")
	@Type(value = Types.TINYINT)
	private Integer drcStatus;

	/**
	 * DRC搭建状态
	 */
	@Column(name = "drc_establish_status")
	@Type(value = Types.TINYINT)
	private Integer drcEstablishStatus;

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
	 * 是否删除, 0:否; 1:是
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
	 * 是否监控, 0:否; 1:是
	 */
	@Column(name = "unit_verification_switch")
	@Type(value = Types.TINYINT)
	private Integer unitVerificationSwitch;

	/**
	 * 是否开启监控, 0:否; 1:是
	 */
	@Column(name = "monitor_switch")
	@Type(value = Types.TINYINT)
	private Integer monitorSwitch;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Integer getDrcStatus() {
		return drcStatus;
	}

	public void setDrcStatus(Integer drcStatus) {
		this.drcStatus = drcStatus;
	}

	public Integer getDrcEstablishStatus() {
		return drcEstablishStatus;
	}

	public void setDrcEstablishStatus(Integer drcEstablishStatus) {
		this.drcEstablishStatus = drcEstablishStatus;
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

	public Integer getUnitVerificationSwitch() {
		return unitVerificationSwitch;
	}

	public void setUnitVerificationSwitch(Integer unitVerificationSwitch) {
		this.unitVerificationSwitch = unitVerificationSwitch;
	}

	public Integer getMonitorSwitch() {
		return monitorSwitch;
	}

	public void setMonitorSwitch(Integer monitorSwitch) {
		this.monitorSwitch = monitorSwitch;
	}
}