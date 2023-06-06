package com.ctrip.framework.drc.console.dao.entity;

import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * @author shb沈海波
 * @date 2020-08-28
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "applier_tbl")
public class ApplierTbl implements DalPojo {

    /**
     * 主键
     */
    @Id
	@Column(name = "id")
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Type(value = Types.BIGINT)
	private Long id;

    /**
     * 端口
     */
	@Column(name = "port")
	@Type(value = Types.INTEGER)
	private Integer port;

    /**
     * DRC资源机器id
     */
	@Column(name = "resource_id")
	@Type(value = Types.BIGINT)
	private Long resourceId;

    /**
     * 主从关系, 0:从, 1:主
     */
	@Column(name = "master")
	@Type(value = Types.TINYINT)
	private Integer master;

    /**
     * applier group id
     */
	@Column(name = "applier_group_id")
	@Type(value = Types.BIGINT)
	private Long applierGroupId;

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
     * 初始gtid
     */
	@Deprecated
	@Column(name = "gtid_init")
	@Type(value = Types.VARCHAR)
	private String gtidInit;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public Long getResourceId() {
		return resourceId;
	}

	public void setResourceId(Long resourceId) {
		this.resourceId = resourceId;
	}

	public Integer getMaster() {
		return master;
	}

	public void setMaster(Integer master) {
		this.master = master;
	}

	public Long getApplierGroupId() {
		return applierGroupId;
	}

	public void setApplierGroupId(Long applierGroupId) {
		this.applierGroupId = applierGroupId;
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

	public String getGtidInit() {
		return gtidInit;
	}

	public void setGtidInit(String gtidInit) {
		this.gtidInit = gtidInit;
	}

}