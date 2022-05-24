package com.ctrip.framework.drc.console.dao;


import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;


import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

/**
 * @author phd潘昊栋
 * @date 2022-04-28
 */
@Repository
public class DataMediaTblDao extends AbstractDao<DataMediaTbl>{

	public DataMediaTblDao() throws SQLException {
		super(DataMediaTbl.class);
	}
	
	public List<DataMediaTbl> queryByIdsAndType (List<Long> dataMediaMappingIds,Integer type, Integer deleted) throws SQLException {
		if(CollectionUtils.isEmpty(dataMediaMappingIds)) {
			throw new IllegalArgumentException("build sql: query DataMediaTbl By MappingIds ,but mappingIds is empty");
		}
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().in("id", dataMediaMappingIds, Types.BIGINT)
				.and().equal("type",type, Types.TINYINT)
				.and().equal("deleted", deleted, Types.TINYINT);
		return client.query(builder,new DalHints());
	}

    public List<DataMediaTbl> queryByDataSourceId(Long srcMhaId, Integer type, Integer deleted) throws SQLException {
		if (srcMhaId == null) {
			throw new IllegalArgumentException("build sql: query DataMediaTbl By ByDataSourceId ,but its is empty");
		}
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().equal("data_media_source_id", srcMhaId, Types.BIGINT)
				.and().equal("type",type, Types.TINYINT)
				.and().equal("deleted", deleted, Types.TINYINT);
		return client.query(builder,new DalHints());
    }


	public List<DataMediaTbl> queryAllByDeleted(Integer deleted) throws SQLException {
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().equal("deleted", deleted, Types.TINYINT);
		return client.query(builder,new DalHints());
	}
}