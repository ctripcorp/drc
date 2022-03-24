package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ApplierTbl;
import com.ctrip.framework.drc.console.dao.entity.GroupMappingTbl;
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
public class ApplierTblDao extends AbstractDao<ApplierTbl> {

	public ApplierTblDao() throws SQLException {
		super(ApplierTbl.class);
	}

	public List<ApplierTbl> queryByApplierGroupIds(List<Long> applierGroupIds , Integer deleted) throws SQLException {
		if (CollectionUtils.isEmpty(applierGroupIds)) {
			throw new IllegalArgumentException("build sql: query ApplierTbl ByApplierGroupIds, but applierGroupIds is empty.");
		}
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().equal("deleted", deleted, Types.TINYINT, false).
				and().in("applier_group_id", applierGroupIds, Types.BIGINT,false);
		return client.query(builder,new DalHints());
	}
}