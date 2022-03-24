package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ClusterTbl;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * @author shb沈海波
 * @date 2020-08-12
 */
public class ClusterTblDao extends AbstractDao<ClusterTbl> {
	
	public ClusterTblDao() throws SQLException {
		super(ClusterTbl.class);
	}
	
	// mysql 'equal' perform the same as 'in' when elements only one
	public List<ClusterTbl> queryByClusterNames(final List<String> clusterNames, final Integer deleted) throws SQLException {
		SelectSqlBuilder builder = new SelectSqlBuilder();
		if (CollectionUtils.isEmpty(clusterNames)) {
			throw new IllegalArgumentException("build sql: query clusters by clusterNames, but clusterNames is empty.");
		}
		builder.selectAll().equal("deleted", deleted, Types.TINYINT, false).
				and().in("cluster_name", clusterNames, Types.VARCHAR,false);
		return client.query(builder,new DalHints());
	}

}