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
 * @author shb沈海波
 * @date 2021-03-23
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "unit_route_verification_history_tbl")
public class UnitRouteVerificationHistoryTbl implements DalPojo {

    /**
     * 主键
     */
    @Id
	@Column(name = "id")
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Type(value = Types.BIGINT)
	private Long id;

    /**
     * 事务id
     */
	@Column(name = "gtid")
	@Type(value = Types.VARCHAR)
	private String gtid;

    /**
     * 执行SQL语句
     */
	@Column(name = "query_sql")
	@Type(value = Types.VARCHAR)
	private String querySql;

    /**
     * 路由策略机房
     */
	@Column(name = "expected_dc")
	@Type(value = Types.VARCHAR)
	private String expectedDc;

    /**
     * 实际写入机房
     */
	@Column(name = "actual_dc")
	@Type(value = Types.VARCHAR)
	private String actualDc;

    /**
     * 表字段列表
     */
	@Column(name = "columns")
	@Type(value = Types.VARCHAR)
	private String columns;

    /**
     * 影响前内容
     */
	@Column(name = "before_values")
	@Type(value = Types.VARCHAR)
	private String beforeValues;

    /**
     * 影响后内容
     */
	@Column(name = "after_values")
	@Type(value = Types.VARCHAR)
	private String afterValues;

    /**
     * 用户id字段名
     */
	@Column(name = "uid_name")
	@Type(value = Types.VARCHAR)
	private String uidName;

    /**
     * ucs策略号
     */
	@Column(name = "ucs_strategy_id")
	@Type(value = Types.INTEGER)
	private Integer ucsStrategyId;

    /**
     * 集群mha group id
     */
	@Column(name = "mha_group_id")
	@Type(value = Types.BIGINT)
	private Long mhaGroupId;

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
     * 执行时间
     */
	@Column(name = "execute_time")
	@Type(value = Types.TIMESTAMP)
	private Timestamp executeTime;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getGtid() {
		return gtid;
	}

	public void setGtid(String gtid) {
		this.gtid = gtid;
	}

	public String getQuerySql() {
		return querySql;
	}

	public void setQuerySql(String querySql) {
		this.querySql = querySql;
	}

	public String getExpectedDc() {
		return expectedDc;
	}

	public void setExpectedDc(String expectedDc) {
		this.expectedDc = expectedDc;
	}

	public String getActualDc() {
		return actualDc;
	}

	public void setActualDc(String actualDc) {
		this.actualDc = actualDc;
	}

	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}

	public String getBeforeValues() {
		return beforeValues;
	}

	public void setBeforeValues(String beforeValues) {
		this.beforeValues = beforeValues;
	}

	public String getAfterValues() {
		return afterValues;
	}

	public void setAfterValues(String afterValues) {
		this.afterValues = afterValues;
	}

	public String getUidName() {
		return uidName;
	}

	public void setUidName(String uidName) {
		this.uidName = uidName;
	}

	public Integer getUcsStrategyId() {
		return ucsStrategyId;
	}

	public void setUcsStrategyId(Integer ucsStrategyId) {
		this.ucsStrategyId = ucsStrategyId;
	}

	public Long getMhaGroupId() {
		return mhaGroupId;
	}

	public void setMhaGroupId(Long mhaGroupId) {
		this.mhaGroupId = mhaGroupId;
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

	public Timestamp getExecuteTime() {
		return executeTime;
	}

	public void setExecuteTime(Timestamp executeTime) {
		this.executeTime = executeTime;
	}

}