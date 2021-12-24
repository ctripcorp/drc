package com.ctrip.framework.drc.console.dao.entity;

import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.*;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * @author shb沈海波
 * @date 2020-03-02
 */
@Entity
@Database(name = "fxdrcmetadb_w")
@Table(name = "gtid_tbl")
public class GtidTbl implements DalPojo {

    /**
     * 空
     */
    @Id
	@Column(name = "gtid_id")
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Type(value = Types.BIGINT)
	private Long gtidId;

    /**
     * 空
     */
	@Column(name = "db_id")
	@Type(value = Types.BIGINT)
	private Long dbId;

    /**
     * 空
     */
	@Column(name = "datachange_lasttime", insertable = false, updatable = false)
	@Type(value = Types.TIMESTAMP)
	private Timestamp datachangeLasttime;

	public Long getGtidId() {
		return gtidId;
	}

	public void setGtidId(Long gtidId) {
		this.gtidId = gtidId;
	}

	public Long getDbId() {
		return dbId;
	}

	public void setDbId(Long dbId) {
		this.dbId = dbId;
	}

	public Timestamp getDatachangeLasttime() {
		return datachangeLasttime;
	}

	public void setDatachangeLasttime(Timestamp datachangeLasttime) {
		this.datachangeLasttime = datachangeLasttime;
	}

}