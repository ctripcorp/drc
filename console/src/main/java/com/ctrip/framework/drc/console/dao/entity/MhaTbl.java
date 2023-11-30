package com.ctrip.framework.drc.console.dao.entity;

import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * @author shb沈海波
 * @date 2020-09-20
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "mha_tbl")
public class MhaTbl implements DalPojo {

    /**
     * 主键
     */
    @Id
	@Column(name = "id")
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Type(value = Types.BIGINT)
	private Long id;

    /**
     * 集群mha名称
     */
	@Column(name = "mha_name")
	@Type(value = Types.VARCHAR)
	private String mhaName;

    /**
     * 集群mha group id，表示复制关系
     */
	@Column(name = "mha_group_id")
	@Type(value = Types.BIGINT)
	private Long mhaGroupId;

    /**
     * 集群mha所在dc id
     */
	@Column(name = "dc_id")
	@Type(value = Types.BIGINT)
	private Long dcId;

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
     * db分机房域名是否创建, 0:否; 1:是
     */
	@Column(name = "dns_status")
	@Type(value = Types.TINYINT)
	private Integer dnsStatus;


	/**
	 * 是否开启监控, 0:否; 1:是
	 * default 0 
	 */
	@Column(name = "monitor_switch")
	@Type(value = Types.TINYINT)
	private Integer monitorSwitch;

	/**
	 * apply mode, 0: set gtid, 1: transaction table,
	 */
	@Column(name = "apply_mode")
	@Type(value = Types.TINYINT)
	private Integer applyMode;

	/**
	 * 标签
	 */
	@Column(name = "tag")
	@Type(value = Types.VARCHAR)
	private String tag;

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

	public String getMhaName() {
		return mhaName;
	}

	public void setMhaName(String mhaName) {
		this.mhaName = mhaName;
	}

	public Long getMhaGroupId() {
		return mhaGroupId;
	}

	public void setMhaGroupId(Long mhaGroupId) {
		this.mhaGroupId = mhaGroupId;
	}

	public Long getDcId() {
		return dcId;
	}

	public void setDcId(Long dcId) {
		this.dcId = dcId;
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

	public Integer getDnsStatus() {
		return dnsStatus;
	}

	public void setDnsStatus(Integer dnsStatus) {
		this.dnsStatus = dnsStatus;
	}

	public Integer getMonitorSwitch() {
		return monitorSwitch;
	}

	public void setMonitorSwitch(Integer monitorSwitch) {
		this.monitorSwitch = monitorSwitch;
	}

	public Integer getApplyMode() {
		return applyMode;
	}

	public void setApplyMode(Integer applyMode) {
		this.applyMode = applyMode;
	}
}