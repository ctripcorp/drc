package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/5/25 12:04
 */
@Repository
public class MhaTblV2Dao extends AbstractDao<MhaTblV2> {

    private static final String MHA_NAME = "mha_name";
    private static final String ID = "id";
    public MhaTblV2Dao() throws SQLException {
        super(MhaTblV2.class);
    }

    public MhaTblV2 queryByMhaName(String mhaName) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(MHA_NAME, mhaName, Types.VARCHAR);
        return client.queryFirst(sqlBuilder, new DalHints());
    }

    public List<MhaTblV2> queryByMhaNames(List<String> mhaNames) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().in(MHA_NAME, mhaNames, Types.VARCHAR);
        return client.query(sqlBuilder, new DalHints());
    }

    @Override
    public List<MhaTblV2> queryByIds(List<Long> ids) throws SQLException {
        if (CollectionUtils.isEmpty(ids)) {
            return Collections.emptyList();
        }
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().in(ID, ids, Types.BIGINT);
        return client.query(sqlBuilder, new DalHints());
    }
}
