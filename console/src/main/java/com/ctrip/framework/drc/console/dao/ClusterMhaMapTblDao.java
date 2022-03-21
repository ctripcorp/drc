package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ClusterMhaMapTbl;
import com.ctrip.framework.drc.console.dao.entity.GroupMappingTbl;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * @author shb沈海波
 * @date 2020-08-11
 */
public class ClusterMhaMapTblDao extends AbstractDao<ClusterMhaMapTbl> {

	public ClusterMhaMapTblDao() throws SQLException {
		super(ClusterMhaMapTbl.class);
	}

	public List<ClusterMhaMapTbl> queryByMhaIds(List<Long> mhaIds , Integer deleted) throws SQLException {
		if (CollectionUtils.isEmpty(mhaIds)) {
			throw new IllegalArgumentException("build sql: query ClusterMhaMapTbl ByMhaIds, but mhaIds is empty.");
		}
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().equal("deleted", deleted, Types.TINYINT, false).
				and().in("mha_id", mhaIds, Types.BIGINT,false);
		return client.query(builder,new DalHints());
	}
}