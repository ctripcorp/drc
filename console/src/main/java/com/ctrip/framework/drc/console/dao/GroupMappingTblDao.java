package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.GroupMappingTbl;
import com.ctrip.platform.dal.dao.sqlbuilder.FreeSelectSqlBuilder;

import java.sql.SQLException;
import java.util.List;

/**
 * @author shb沈海波
 * @date 2021-05-28
 */
public class GroupMappingTblDao extends AbstractDao<GroupMappingTbl> {
	public GroupMappingTblDao() throws SQLException {
		super(GroupMappingTbl.class);
	}
}