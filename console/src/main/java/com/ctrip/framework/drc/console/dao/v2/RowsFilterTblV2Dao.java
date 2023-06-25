package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.RowsFilterTblV2;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/6/25 16:54
 */
@Repository
public class RowsFilterTblV2Dao extends AbstractDao<RowsFilterTblV2> {

    private static final String ID = "id";
    private static final String DELETED = "deleted";
    private static final String MODE = "mode";
    private static final String CONFIGS = "configs";

    public RowsFilterTblV2Dao() throws SQLException {
        super(RowsFilterTblV2.class);
    }

    public List<RowsFilterTblV2> queryByIds(List<Long> ids) throws SQLException {
        if (CollectionUtils.isEmpty(ids)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().in(ID, ids, Types.BIGINT).and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return client.query(sqlBuilder, new DalHints());
    }

    public RowsFilterTblV2 queryByConfigs(int mode, String configs) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(MODE, mode, Types.TINYINT).and().equal(CONFIGS, configs, Types.VARCHAR);
        return client.queryFirst(sqlBuilder, new DalHints());
    }
}
