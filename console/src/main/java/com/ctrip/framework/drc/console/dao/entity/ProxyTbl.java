package com.ctrip.framework.drc.console.dao.entity;
import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * @author shb沈海波
 * @date 2021-05-12
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "proxy_tbl")
public class ProxyTbl implements DalPojo {

    /**
     * primary key
     */
    @Id
	@Column(name = "id")
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Type(value = Types.BIGINT)
	private Long id;

    /**
     * dc id
     */
	@Column(name = "dc_id")
	@Type(value = Types.BIGINT)
	private Long dcId;

    /**
     * scheme, like PROXYTCP, PROXYTLS://127.0.0.1:8080, TCP://127.0.0.1:8090
     */
	@Column(name = "uri")
	@Type(value = Types.VARCHAR)
	private String uri;

    /**
     * active or not
     */
	@Column(name = "active")
	@Type(value = Types.TINYINT)
	private Integer active;

    /**
     * monitor this proxy or not
     */
	@Column(name = "monitor_active")
	@Type(value = Types.TINYINT)
	private Integer monitorActive;

    /**
     * deleted or not
     */
	@Column(name = "deleted")
	@Type(value = Types.TINYINT)
	private Integer deleted;

    /**
     * create time
     */
	@Column(name = "create_time")
	@Type(value = Types.TIMESTAMP)
	private Timestamp createTime;

    /**
     * data changed last time
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

	public Long getDcId() {
		return dcId;
	}

	public void setDcId(Long dcId) {
		this.dcId = dcId;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public Integer getActive() {
		return active;
	}

	public void setActive(Integer active) {
		this.active = active;
	}

	public Integer getMonitorActive() {
		return monitorActive;
	}

	public void setMonitorActive(Integer monitorActive) {
		this.monitorActive = monitorActive;
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