package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.GroupMappingTbl;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.FreeSelectSqlBuilder;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * @author shb沈海波
 * @date 2021-05-28
 */
public class GroupMappingTblDao extends AbstractDao<GroupMappingTbl> {
	public GroupMappingTblDao() throws SQLException {
		super(GroupMappingTbl.class);
	}
	
	public List<GroupMappingTbl> queryByMhaGroupIds(List<Long> mhaGroupIds ,Integer deleted) throws SQLException {
		if (CollectionUtils.isEmpty(mhaGroupIds)) {
			throw new IllegalArgumentException("build sql: queryByMhaGroupIds, but mhaGroupIds is empty.");
		}
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().equal("deleted", deleted, Types.TINYINT, false).
				and().in("mha_group_id", mhaGroupIds, Types.BIGINT,false);
		return client.query(builder,new DalHints());
	}
}