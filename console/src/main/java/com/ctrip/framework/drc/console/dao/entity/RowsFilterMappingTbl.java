package com.ctrip.framework.drc.console.dao.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Sensitive;
import com.ctrip.platform.dal.dao.annotation.Type;
import java.sql.Types;
import java.sql.Timestamp;

import com.ctrip.platform.dal.dao.DalPojo;

/**
 * @author phd潘昊栋
 * @date 2022-04-28
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "rows_filter_mapping_tbl")
public class RowsFilterMappingTbl implements DalPojo {

    /**
     * pk
     */
    @Id
	@Column(name = "id")
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Type(value = Types.BIGINT)
	private Long id;

    /**
     * data_media_index
     */
	@Column(name = "data_media_id")
	@Type(value = Types.BIGINT)
	private Long dataMediaId;

    /**
     * rows_filter_index
     */
	@Column(name = "rows_filter_id")
	@Type(value = Types.BIGINT)
	private Long rowsFilterId;


	@Column(name = "applier_group_id")
	@Type(value = Types.BIGINT)
	private Long applierGroupId;


	/**
	 * 0 applier，3 messenger
	 * default 0, see ConsumerType
	 */
	@Column(name = "type")
	@Type(value = Types.TINYINT)
	private Integer type;

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

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getDataMediaId() {
		return dataMediaId;
	}

	public void setDataMediaId(Long dataMediaId) {
		this.dataMediaId = dataMediaId;
	}

	public Long getRowsFilterId() {
		return rowsFilterId;
	}

	public void setRowsFilterId(Long rowsFilterId) {
		this.rowsFilterId = rowsFilterId;
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

	public Long getApplierGroupId() {
		return applierGroupId;
	}

	public void setApplierGroupId(Long applierGroupId) {
		this.applierGroupId = applierGroupId;
	}

	public Integer getType() {
		return type;
	}

	public void setType(Integer type) {
		this.type = type;
	}
}