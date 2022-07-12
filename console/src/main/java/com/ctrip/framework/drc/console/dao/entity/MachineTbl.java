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
@Table(name = "machine_tbl")
public class MachineTbl implements DalPojo {

	/**
	 * 主键
	 */
	@Id
	@Column(name = "id")
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Type(value = Types.BIGINT)
	private Long id;

	/**
	 * MySQL实例ip
	 */
	@Column(name = "ip")
	@Type(value = Types.VARCHAR)
	private String ip;

	/**
	 * MySQL实例端口
	 */
	@Column(name = "port")
	@Type(value = Types.INTEGER)
	private Integer port;

	/**
	 * MySQL实例uuid
	 */
	@Column(name = "uuid")
	@Type(value = Types.VARCHAR)
	private String uuid;

	/**
	 * 主从关系, 0:从, 1:主
	 */
	@Column(name = "master")
	@Type(value = Types.TINYINT)
	private Integer master;

	/**
	 * 集群mha id
	 */
	@Column(name = "mha_id")
	@Type(value = Types.BIGINT)
	private Long mhaId;

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

	public MachineTbl() {
	}

	public MachineTbl(String ip, Integer port, Integer master) {
		this.ip = ip;
		this.port = port;
		this.master = master;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public Integer getMaster() {
		return master;
	}

	public void setMaster(Integer master) {
		this.master = master;
	}

	public Long getMhaId() {
		return mhaId;
	}

	public void setMhaId(Long mhaId) {
		this.mhaId = mhaId;
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