package com.ctrip.framework.drc.console.dao.entity;

import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * @author shb沈海波
 * @date 2021-03-25
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "applier_group_tbl")
public class ApplierGroupTbl implements DalPojo {

    /**
     * 主键
     */
    @Id
	@Column(name = "id")
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Type(value = Types.BIGINT)
	private Long id;

    /**
     * replicator group id
     */
	@Column(name = "replicator_group_id")
	@Type(value = Types.BIGINT)
	private Long replicatorGroupId;

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

    /**
     * request db list, seprated by commas,
     */
	@Column(name = "includedDbs")
	@Type(value = Types.VARCHAR)
	private String includedDbs;

	/**
	 * apply mode, 0: set gtid, 1: transaction table,
	 */
	@Column(name = "apply_mode")
	@Type(value = Types.TINYINT)
	private Integer applyMode;

	/**
	 * table name filter, seprated by commas,
	 */
	@Column(name = "name_filter")
	@Type(value = Types.VARCHAR)
	private String nameFilter;

	/**
	 * table name mapping
	 */
	@Column(name = "name_mapping")
	@Type(value = Types.VARCHAR)
	private String nameMapping;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getReplicatorGroupId() {
		return replicatorGroupId;
	}

	public void setReplicatorGroupId(Long replicatorGroupId) {
		this.replicatorGroupId = replicatorGroupId;
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

	public String getIncludedDbs() {
		return includedDbs;
	}

	public void setIncludedDbs(String includedDbs) {
		this.includedDbs = includedDbs;
	}

	public Integer getApplyMode() {
		return applyMode;
	}

	public void setApplyMode(Integer applyMode) {
		this.applyMode = applyMode;
	}

	public String getNameFilter() {
		return nameFilter;
	}

	public void setNameFilter(String nameFilter) {
		this.nameFilter = nameFilter;
	}

	public String getNameMapping() {
		return nameMapping;
	}

	public void setNameMapping(String nameMapping) {
		this.nameMapping = nameMapping;
	}
}
