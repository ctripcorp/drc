package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ClusterManagerTbl;

import java.sql.SQLException;

/**
 * @author shb沈海波
 * @date 2020-08-11
 */
public class ClusterManagerTblDao extends AbstractDao<ClusterManagerTbl> {
	
	public ClusterManagerTblDao() throws SQLException {
		super(ClusterManagerTbl.class);
	}

}