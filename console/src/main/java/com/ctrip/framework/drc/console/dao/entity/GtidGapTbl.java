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
@Table(name = "gtid_gap_tbl")
public class GtidGapTbl implements DalPojo {

    /**
     * 空
     */
    @Id
	@Column(name = "id")
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Type(value = Types.BIGINT)
	private Long id;

    /**
     * 空
     */
	@Column(name = "gtid_id")
	@Type(value = Types.BIGINT)
	private Long gtidId;

    /**
     * 空
     */
	@Column(name = "gap_interval_start")
	@Type(value = Types.BIGINT)
	private Long gapIntervalStart;

    /**
     * 空
     */
	@Column(name = "gap_interval_end")
	@Type(value = Types.BIGINT)
	private Long gapIntervalEnd;

    /**
     * 空
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

	public Long getGtidId() {
		return gtidId;
	}

	public void setGtidId(Long gtidId) {
		this.gtidId = gtidId;
	}

	public Long getGapIntervalStart() {
		return gapIntervalStart;
	}

	public void setGapIntervalStart(Long gapIntervalStart) {
		this.gapIntervalStart = gapIntervalStart;
	}

	public Long getGapIntervalEnd() {
		return gapIntervalEnd;
	}

	public void setGapIntervalEnd(Long gapIntervalEnd) {
		this.gapIntervalEnd = gapIntervalEnd;
	}

	public Timestamp getDatachangeLasttime() {
		return datachangeLasttime;
	}

	public void setDatachangeLasttime(Timestamp datachangeLasttime) {
		this.datachangeLasttime = datachangeLasttime;
	}

}