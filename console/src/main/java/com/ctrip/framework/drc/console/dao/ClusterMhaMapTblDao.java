package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ClusterMhaMapTbl;

import java.sql.SQLException;

/**
 * @author shb沈海波
 * @date 2020-08-11
 */
public class ClusterMhaMapTblDao extends AbstractDao<ClusterMhaMapTbl> {

	public ClusterMhaMapTblDao() throws SQLException {
		super(ClusterMhaMapTbl.class);
	}

}