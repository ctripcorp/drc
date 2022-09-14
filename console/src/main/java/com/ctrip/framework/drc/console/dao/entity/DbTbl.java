package com.ctrip.framework.drc.console.dao.entity;

import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * @author shb沈海波
 * @date 2020-08-11
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "db_tbl")
public class DbTbl implements DalPojo {

    /**
     * 主键
     */
    @Id
	@Column(name = "id")
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Type(value = Types.BIGINT)
	private Long id;

    /**
     * 数据库名
     */
	@Column(name = "db_name")
	@Type(value = Types.VARCHAR)
	private String dbName;

    /**
     * Db owner
     */
	@Column(name = "db_owner")
	@Type(value = Types.VARCHAR)
	private String dbOwner;

    /**
     * buCode 如 FLT
     */
	@Column(name = "bu_code")
	@Type(value = Types.VARCHAR)
	private String buCode;

	/**
	 * buName 中文名
	 */
	@Column(name = "bu_name")
	@Type(value = Types.VARCHAR)
	private String buName;

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
	 * 上次发送流量统计时间，unix秒数，round to hour
	 */
	@Column(name = "traffic_send_last_time")
	@Type(value = Types.BIGINT)
	private Long trafficSendLastTime;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getDbOwner() {
		return dbOwner;
	}

	public void setDbOwner(String dbOwner) {
		this.dbOwner = dbOwner;
	}

	public String getBuCode() {
		return buCode;
	}

	public void setBuCode(String buCode) {
		this.buCode = buCode;
	}

	public String getBuName() {
		return buName;
	}

	public void setBuName(String buName) {
		this.buName = buName;
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

	public Long getTrafficSendLastTime() {
		return trafficSendLastTime;
	}

	public void setTrafficSendLastTime(Long trafficSendLastTime) {
		this.trafficSendLastTime = trafficSendLastTime;
	}
}
