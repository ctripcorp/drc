package com.ctrip.framework.drc.console.dao;


import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * @author phd潘昊栋
 * @date 2022-04-28
 */
@Repository
public class DataMediaTblDao extends AbstractDao<DataMediaTbl>{

	public DataMediaTblDao() throws SQLException {
		super(DataMediaTbl.class);
	}
	
	public Long insertReturnPk(DataMediaTbl dataMediaTbl) throws SQLException{
		KeyHolder keyHolder = new KeyHolder();
		insert(new DalHints(), keyHolder, dataMediaTbl);
		return (Long) keyHolder.getKey();
	}

	public List<DataMediaTbl> queryByAGroupId(Long applierGroupId, Integer deleted) throws SQLException {
		if (applierGroupId == null) {
			throw new IllegalArgumentException("build sql: query DataMediaTbl By AGroupId ,but it is empty");
		}
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().equal("applier_group_id", applierGroupId, Types.BIGINT)
				.and().equal("deleted", deleted, Types.TINYINT);
		return client.query(builder,new DalHints());
	}
	
	public List<DataMediaTbl> queryByIdsAndType (List<Long> dataMediaIds,Integer type, Integer deleted) throws SQLException {
		if(CollectionUtils.isEmpty(dataMediaIds)) {
			throw new IllegalArgumentException("build sql: query DataMediaTbl By MappingIds ,but mappingIds is empty");
		}
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().in("id", dataMediaIds, Types.BIGINT)
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

	public DataMediaTbl queryById(Long id) throws SQLException {
		SelectSqlBuilder sqlBuilder = initSqlBuilder();
		sqlBuilder.and().equal("id", id, Types.BIGINT);
		return client.queryFirst(sqlBuilder, new DalHints());
	}
}