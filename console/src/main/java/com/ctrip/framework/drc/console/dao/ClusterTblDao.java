package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ClusterTbl;

import java.sql.SQLException;

/**
 * @author shb沈海波
 * @date 2020-08-12
 */
public class ClusterTblDao extends AbstractDao<ClusterTbl> {
	
	public ClusterTblDao() throws SQLException {
		super(ClusterTbl.class);
	}

}