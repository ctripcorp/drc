package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ReplicatorTbl;

import java.sql.SQLException;

/**
 * @author shb沈海波
 * @date 2020-08-28
 */
public class ReplicatorTblDao extends AbstractDao<ReplicatorTbl> {

	public ReplicatorTblDao() throws SQLException {
		super(ReplicatorTbl.class);
	}

}