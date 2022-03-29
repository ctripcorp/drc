package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ApplierTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorGroupTbl;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * @author shb沈海波
 * @date 2020-08-28
 */
public class ReplicatorGroupTblDao  extends AbstractDao<ReplicatorGroupTbl> {

	public ReplicatorGroupTblDao() throws SQLException {
		super(ReplicatorGroupTbl.class);
	}
	
	//mhaId which replicator fetch binlog
	public List<ReplicatorGroupTbl> queryByMhaIds (List<Long> mhaIds , Integer deleted) throws SQLException {
		if (CollectionUtils.isEmpty(mhaIds)) {
			throw new IllegalArgumentException("build sql: query ReplicatorGroupTbl ByMhaIds, but mhaIds is empty.");
		}
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().equal("deleted", deleted, Types.TINYINT, false).
				and().in("mha_id", mhaIds, Types.BIGINT, false);
		return client.query(builder, new DalHints());
	}
	
}