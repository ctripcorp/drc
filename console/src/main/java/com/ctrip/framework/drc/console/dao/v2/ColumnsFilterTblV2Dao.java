package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.ColumnsFilterTblV2;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/5/25 11:59
 */
@Repository
public class ColumnsFilterTblV2Dao extends AbstractDao<ColumnsFilterTblV2> {

    private static final String ID = "id";
    private static final String DELETED = "deleted";
    private static final String MODE = "mode";
    private static final String COLUMNS = "columns";

    public ColumnsFilterTblV2Dao() throws SQLException {
        super(ColumnsFilterTblV2.class);
    }

    public List<ColumnsFilterTblV2> queryByIds(List<Long> ids) throws SQLException {
        if (CollectionUtils.isEmpty(ids)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().in(ID, ids, Types.BIGINT).and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return client.query(sqlBuilder, new DalHints());
    }

    public ColumnsFilterTblV2 queryOneByColumns(int mode, String columns) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(MODE, mode, Types.TINYINT).and().equal(COLUMNS, columns, Types.VARCHAR);
        return client.queryFirst(sqlBuilder, new DalHints());
    }

    public List<ColumnsFilterTblV2> queryByColumns(int mode, String columns) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(MODE, mode, Types.TINYINT).and().equal(COLUMNS, columns, Types.VARCHAR);
        return client.query(sqlBuilder, new DalHints());
    }

    public List<ColumnsFilterTblV2> queryByMode(int mode) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(MODE, mode, Types.TINYINT);
        return client.query(sqlBuilder, new DalHints());
    }

    public Long insertReturnId(ColumnsFilterTblV2 columnsFilterTbl) throws SQLException {
        KeyHolder keyHolder = new KeyHolder();
        insert(new DalHints(), keyHolder, columnsFilterTbl);
        return (Long) keyHolder.getKey();
    }

}
