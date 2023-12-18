package com.ctrip.framework.drc.console.dao.entity;

import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * @author shb沈海波
 * @date 2021-01-04
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "ddl_history_tbl")
public class DdlHistoryTbl implements DalPojo {

    /**
     * 主键
     */
    @Id
	@Column(name = "id")
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Type(value = Types.BIGINT)
	private Long id;

    /**
     * mha id
     */
	@Column(name = "mha_id")
	@Type(value = Types.BIGINT)
	private Long mhaId;

    /**
     * ddl信息
     */
	@Column(name = "ddl")
	@Type(value = Types.VARCHAR)
	private String ddl;

    /**
     * 库名
     */
	@Column(name = "schema_name")
	@Type(value = Types.VARCHAR)
	private String schemaName;

    /**
     * 表名
     */
	@Column(name = "table_name")
	@Type(value = Types.VARCHAR)
	private String tableName;

    /**
     * ddl类型
     */
	@Column(name = "query_type")
	@Type(value = Types.TINYINT)
	private Integer queryType;

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

	public static DdlHistoryTbl createDdlHistoryPojo(long mhaId, String ddl, int queryType, String schemaName, String tableName) {
		DdlHistoryTbl daoPojo = new DdlHistoryTbl();
		daoPojo.setMhaId(mhaId);
		daoPojo.setDdl(ddl);
		daoPojo.setQueryType(queryType);
		daoPojo.setSchemaName(schemaName);
		daoPojo.setTableName(tableName);
		return daoPojo;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getMhaId() {
		return mhaId;
	}

	public void setMhaId(Long mhaId) {
		this.mhaId = mhaId;
	}

	public String getDdl() {
		return ddl;
	}

	public void setDdl(String ddl) {
		this.ddl = ddl;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Integer getQueryType() {
		return queryType;
	}

	public void setQueryType(Integer queryType) {
		this.queryType = queryType;
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