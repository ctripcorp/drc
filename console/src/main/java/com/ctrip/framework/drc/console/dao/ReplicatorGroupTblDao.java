package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ReplicatorGroupTbl;

import java.sql.SQLException;

/**
 * @author shb沈海波
 * @date 2020-08-28
 */
public class ReplicatorGroupTblDao  extends AbstractDao<ReplicatorGroupTbl> {

	public ReplicatorGroupTblDao() throws SQLException {
		super(ReplicatorGroupTbl.class);
	}

}