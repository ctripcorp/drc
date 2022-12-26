package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ReplicatorTbl;

import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import org.springframework.util.CollectionUtils;

/**
 * @author shb沈海波
 * @date 2020-08-28
 */
public class ReplicatorTblDao extends AbstractDao<ReplicatorTbl> {

	public ReplicatorTblDao() throws SQLException {
		super(ReplicatorTbl.class);
	}

	public List<ReplicatorTbl> queryByRGroupIds(List<Long> replicatorGroupIds , Integer deleted) throws SQLException {
		if (CollectionUtils.isEmpty(replicatorGroupIds)) {
			throw new IllegalArgumentException("build sql: query ReplicatorTbls byRGroupIds, but rGroupIds is empty.");
		}
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().in("relicator_group_id", replicatorGroupIds, Types.BIGINT, false)
				.and().equal("deleted", deleted, Types.TINYINT, false);
		return client.query(builder,new DalHints());
	}
	
}