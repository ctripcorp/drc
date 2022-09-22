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
 * @author phd潘昊栋
 * @date 2022-04-28
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "rows_filter_tbl")
public class RowsFilterTbl implements DalPojo {

    /**
     * primary key
     */
    @Id
	@Column(name = "id")
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Type(value = Types.BIGINT)
	private Long id;

    /**
     * 行过滤配置 模式 regex, trip_uid,customized
     */
	@Column(name = "mode")
	@Type(value = Types.VARCHAR)
	private String mode;

    /**
     * json 保存 columns,context属性
	 * deprecated ,use configs
     */
	@Column(name = "parameters")
	@Type(value = Types.LONGVARCHAR)
	private String parameters;


	/**
	 * json (List of parameters)
	 * parameters contain:
	 * 1.column 2.illegalArgument 3.fetchMode 4.context 5.userFilterMode
	 */
	@Column(name = "configs")
	@Type(value = Types.LONGVARCHAR)
	private String configs;
	
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
	

	public String getMode() {
		return mode;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

	public String getParameters() {
		return parameters;
	}

	public void setParameters(String parameters) {
		this.parameters = parameters;
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

	public String getConfigs() {
		return configs;
	}

	public void setConfigs(String configs) {
		this.configs = configs;
	}
}