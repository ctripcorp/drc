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
@Table(name = "resource_tbl")
public class ResourceTbl implements DalPojo {

    /**
     * 主键
     */
    @Id
	@Column(name = "id")
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Type(value = Types.BIGINT)
	private Long id;

    /**
     * 机器所在appid
     */
	@Column(name = "app_id")
	@Type(value = Types.BIGINT)
	private Long appId;

    /**
     * 机器ip
     */
	@Column(name = "ip")
	@Type(value = Types.VARCHAR)
	private String ip;

    /**
     * dc id
     */
	@Column(name = "dc_id")
	@Type(value = Types.BIGINT)
	private Long dcId;

    /**
     * 机器所在app应用类型
     */
	@Column(name = "type")
	@Type(value = Types.TINYINT)
	private Integer type;

	/**
	 * az
	 */
	@Column(name = "az")
	@Type(value = Types.VARCHAR)
	private String az;

	/**
	 * 机器标签
	 */
	@Column(name = "tag")
	@Type(value = Types.VARCHAR)
	private String tag;

    /**
     * 是否删除, 0:否; 1:是
     */
	@Column(name = "deleted")
	@Type(value = Types.TINYINT)
	private Integer deleted;

	/**
	 * 是否可用, 0:否; 1:是
	 */
	@Column(name = "active")
	@Type(value = Types.TINYINT)
	private Integer active;

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

	public Integer getActive() {
		return active;
	}

	public void setActive(Integer active) {
		this.active = active;
	}

	public String getAz() {
		return az;
	}

	public void setAz(String az) {
		this.az = az;
	}

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

	public Long getAppId() {
		return appId;
	}

	public void setAppId(Long appId) {
		this.appId = appId;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public Long getDcId() {
		return dcId;
	}

	public void setDcId(Long dcId) {
		this.dcId = dcId;
	}

	public Integer getType() {
		return type;
	}

	public void setType(Integer type) {
		this.type = type;
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