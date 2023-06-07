package com.ctrip.framework.drc.console.dao;


import com.ctrip.platform.dal.dao.*;
import com.ctrip.platform.dal.dao.sqlbuilder.*;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMappingTbl;

import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

/**
 * @author phd潘昊栋
 * @date 2022-04-28
 */
@Repository
public class RowsFilterMappingTblDao extends AbstractDao<RowsFilterMappingTbl> {

	public RowsFilterMappingTblDao() throws SQLException {
		super(RowsFilterMappingTbl.class);
	}

	public List<RowsFilterMappingTbl> queryByApplierGroupIds (List<Long> applierGroupIds, Integer deleted) throws SQLException {
		if (CollectionUtils.isEmpty(applierGroupIds)) {
			throw new IllegalArgumentException("build sql: query RowsFilterMappingTbl By applierGroupIds, but applierGroupIds is empty.");
		}
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().in("applier_group_id",applierGroupIds,Types.BIGINT)
				.and().equal("deleted", deleted, Types.TINYINT);
		return client.query(builder,new DalHints());
	}
	
	public List<RowsFilterMappingTbl> queryBy(Long groupId,int applierType,Integer deleted)throws SQLException {
		if (null == groupId) {
			throw new IllegalArgumentException("build sql: query RowsFilterMappingTbl By groupId and type, but groupId is empty.");
		}
		RowsFilterMappingTbl sample = new RowsFilterMappingTbl();
		sample.setApplierGroupId(groupId);
		sample.setType(applierType);
		sample.setDeleted(deleted);
		return queryBy(sample);
	}
		
    public List<RowsFilterMappingTbl> queryByDataMediaIds(List<Long> dataMediaIds, Integer deleted) throws SQLException {
		if (CollectionUtils.isEmpty(dataMediaIds)) {
			throw new IllegalArgumentException("build sql: query RowsFilterMappingTbl By dataMediaIds, but dataMediaIds is empty.");
		}
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().in("data_media_id",dataMediaIds,Types.BIGINT)
				.and().equal("deleted", deleted, Types.TINYINT);
		return client.query(builder,new DalHints());
    }
}