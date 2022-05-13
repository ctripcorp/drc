package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.GroupMappingTbl;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author shb沈海波
 * @date 2021-05-28
 */
public class GroupMappingTblDao extends AbstractDao<GroupMappingTbl> {
	public static final Logger logger = LoggerFactory.getLogger(GroupMappingTblDao.class);
	
	public GroupMappingTblDao() throws SQLException {
		super(GroupMappingTbl.class);
	}
	
	public List<GroupMappingTbl> queryByMhaGroupIds(List<Long> mhaGroupIds ,Integer deleted) throws SQLException {
		if (CollectionUtils.isEmpty(mhaGroupIds)) {
			throw new IllegalArgumentException("build sql: query GroupMappingTbl ByMhaGroupIds, but mhaGroupIds is empty.");
		}
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().in("mha_group_id", mhaGroupIds, Types.BIGINT,false)
				.and().equal("deleted", deleted, Types.TINYINT, false);
		return client.query(builder,new DalHints());
	}

	public List<GroupMappingTbl> queryByMhaIds(List<Long> mhaIds ,Integer deleted) throws SQLException {
		if (CollectionUtils.isEmpty(mhaIds)) {
			throw new IllegalArgumentException("build sql: query GroupMappingTbl By mhaIds, but mhaIds is empty.");
		}
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().in("mha_id", mhaIds, Types.BIGINT,false)
				.and().equal("deleted", deleted, Types.TINYINT, false);
		return client.query(builder,new DalHints());
	}
	
	public Long queryMhaGroupIdByTwoMhaIds(Long mhaId0,Long mhaId1,Integer deleted) throws SQLException {
		if (mhaId0 == null || mhaId1 == null) {
			throw new IllegalArgumentException("build sql: queryMhaGroupIdByTwoMhaIds, but one mhaId is empty.");
		}
		Set<Long> mhaGroupIds0 = queryByMhaIds(Lists.newArrayList(mhaId0), deleted)
				.stream().map(GroupMappingTbl::getMhaGroupId).collect(Collectors.toSet());
		Set<Long> mhaGroupIds1 = queryByMhaIds(Lists.newArrayList(mhaId1), deleted)
				.stream().map(GroupMappingTbl::getMhaGroupId).collect(Collectors.toSet());
		mhaGroupIds0.retainAll(mhaGroupIds1);
		if (mhaGroupIds0.size() == 1) {
			return mhaGroupIds0.iterator().next();
		}
		logger.warn("group for {}-{} find not one but {}",mhaId0,mhaId1,mhaGroupIds0);
		return null;
	}
}