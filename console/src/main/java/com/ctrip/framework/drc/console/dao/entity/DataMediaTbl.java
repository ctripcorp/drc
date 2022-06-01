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
@Table(name = "data_media_tbl")
public class DataMediaTbl implements DalPojo {

    /**
     * pk
     */
    @Id
	@Column(name = "id")
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Type(value = Types.BIGINT)
	private Long id;

    /**
     * schema,topic等，逻辑概念
     */
	@Column(name = "namespcae")
	@Type(value = Types.VARCHAR)
	private String namespcae;

    /**
     * 表名，逻辑概念
     */
	@Column(name = "name")
	@Type(value = Types.VARCHAR)
	private String name;

    /**
     * 0 正则逻辑表，1 映射逻辑表
     */
	@Column(name = "type")
	@Type(value = Types.TINYINT)
	private Integer type;

    /**
     * mysql 使用mha_id,索引列
     */
	@Column(name = "data_media_source_id")
	@Type(value = Types.BIGINT)
	private Long dataMediaSourceId;

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

	public String getFullName() {
		return namespcae + "\\." + name;
	}
	
	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getNamespcae() {
		return namespcae;
	}

	public void setNamespcae(String namespcae) {
		this.namespcae = namespcae;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getType() {
		return type;
	}

	public void setType(Integer type) {
		this.type = type;
	}

	public Long getDataMediaSourceId() {
		return dataMediaSourceId;
	}

	public void setDataMediaSourceId(Long dataMediaSourceId) {
		this.dataMediaSourceId = dataMediaSourceId;
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